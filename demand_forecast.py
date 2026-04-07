import os
import pickle

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    classification_report, roc_auc_score, precision_recall_curve,
)
import lightgbm as lgb
import xgboost as xgb

from utils import init, make_log, get_engine
init()

OUT = "outputs/forecast"
MDL = "outputs/forecast/models"
os.makedirs(OUT, exist_ok=True)
os.makedirs(MDL, exist_ok=True)

PRED_HORIZON = 1      # 다음 1시간 대여량 예측 (2시간도 해봤는데 오차가 커서 1시간으로)
TEST_DAYS    = 14     # 마지막 2주를 테스트셋으로 분리
SHORTAGE_Q   = 0.75   # 75분위 초과 + 순유출 동시 발생을 부족으로 정의

BASE_FEATURE_COLS = [
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos",
    "is_weekend",
    "ma_7d_rentals", "ma_30d_rentals", "ma_7d_netflow", "ma_30d_netflow",
    "shortage_risk", "excess_risk", "avg_net_outflow_7d",
    "rentals_lag_24h", "netflow_lag_24h", "rentals_lag_1w",
    "total_slots",
    # "lat", "lng",  # 넣어봤는데 성능 차이가 별로 없어서 제거
]
ENCODED_COLS = ["district_enc"]
FEATURE_COLS = BASE_FEATURE_COLS + ENCODED_COLS

def load_and_prepare(engine) -> tuple:
    # 최근 90일만 잘라서 학습용 데이터 구성
    sql = """
        SELECT *
        FROM v_features_for_ml
        WHERE record_date >= (
            SELECT DATE_SUB(MAX(record_date), INTERVAL 89 DAY) FROM tb_daily_agg
        )
        ORDER BY station_id, record_date, hour
    """
    df = pd.read_sql(sql, engine, parse_dates=["record_date"])
    print(f"  행: {len(df):,}  |  대여소: {df['station_id'].nunique():,}"
          f"  |  기간: {df['record_date'].min().date()} ~ {df['record_date'].max().date()}")

    # 순환 인코딩
    pi2 = 2 * np.pi
    df["hour_sin"]  = np.sin(pi2 * df["hour"]        / 24)
    df["hour_cos"]  = np.cos(pi2 * df["hour"]        / 24)
    df["dow_sin"]   = np.sin(pi2 * df["day_of_week"] / 7)
    df["dow_cos"]   = np.cos(pi2 * df["day_of_week"] / 7)
    df["month_sin"] = np.sin(pi2 * df["record_date"].dt.month / 12)
    df["month_cos"] = np.cos(pi2 * df["record_date"].dt.month / 12)

    df["target_rentals"]  = df.groupby("station_id")["rentals"].shift(-PRED_HORIZON)
    df["future_net_flow"] = df.groupby("station_id")["net_flow"].shift(-PRED_HORIZON)
    df = df.dropna(subset=BASE_FEATURE_COLS + ["target_rentals", "future_net_flow"])

    cutoff = df["record_date"].max() - pd.Timedelta(days=TEST_DAYS)
    train  = df[df["record_date"] <= cutoff].copy()
    test   = df[df["record_date"] >  cutoff].copy()

    q_rentals = train.groupby("station_id")["rentals"].quantile(SHORTAGE_Q).rename("q_rentals")
    q_netflow = (train.groupby("station_id")["net_flow"]
                 .quantile(SHORTAGE_Q).clip(lower=0).rename("q_netflow"))

    for split in [train, test]:
        split["q_rentals"] = split["station_id"].map(q_rentals)
        split["q_netflow"] = split["station_id"].map(q_netflow)
        split["target_shortage"] = (
            (split["target_rentals"]  > split["q_rentals"]) &
            (split["future_net_flow"] > split["q_netflow"])
        ).astype("int8")

    print(f"  부족 판정 분위수: {SHORTAGE_Q:.0%}  "
          f"| 부족 비율 — 학습: {train['target_shortage'].mean():.3f}  "
          f"테스트: {test['target_shortage'].mean():.3f}")

    # 미지 구역 기타 처리
    le = LabelEncoder()
    all_districts = sorted(train["district"].dropna().unique().tolist()) + ["기타"]
    le.fit(all_districts)
    known = set(le.classes_)

    for split in [train, test]:
        col = split["district"].where(split["district"].isin(known), other="기타")
        split["district_enc"] = le.transform(col).astype("int16")

    return (
        train[FEATURE_COLS], test[FEATURE_COLS],
        train["target_rentals"],  test["target_rentals"],
        train["target_shortage"], test["target_shortage"],
        test, le,
    )

# 수요 예측 모델 (LightGBM 회귀)
# 랜덤포레스트 대비 속도·성능 균형이 나아서 선택, early stopping으로 과적합 조절

def train_regression(X_tr, y_tr, X_te, y_te):
    # TODO: optuna로 하이퍼파라미터 탐색해보고 싶은데 일단 수동으로
    model = lgb.LGBMRegressor(
        n_estimators=600,    # early stopping 기준으로 300 → 600으로 조정
        learning_rate=0.05,
        num_leaves=63,       # max_depth=7과 세트로 과적합 조절
        max_depth=7,
        min_child_samples=20,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, verbose=-1,
    )
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    pred = model.predict(X_te).clip(0)
    print(f"\n[LightGBM 회귀]  MAE {mean_absolute_error(y_te, pred):.3f}"
          f"  RMSE {np.sqrt(mean_squared_error(y_te, pred)):.3f}"
          f"  R² {r2_score(y_te, pred):.4f}")
    return model, pred


# 부족 발생 분류 모델
# LightGBM 분류도 비교했지만 불균형 클래스 처리(scale_pos_weight)가 XGBoost가 더 직관적이었음

def _find_optimal_threshold(y_true: np.ndarray, prob: np.ndarray) -> tuple[float, float]:
    """PR 커브에서 F1 최대 임계값 반환."""
    precisions, recalls, thresholds = precision_recall_curve(y_true, prob)
    f1s = 2 * precisions[:-1] * recalls[:-1] / (precisions[:-1] + recalls[:-1] + 1e-8)
    best_idx = int(np.argmax(f1s))
    return float(thresholds[best_idx]), float(f1s[best_idx])


def train_classifier(X_tr, y_tr, X_te, y_te):
    neg, pos = (y_tr == 0).sum(), (y_tr == 1).sum()
    model = xgb.XGBClassifier(
        n_estimators=400, learning_rate=0.05, max_depth=6,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=neg / max(pos, 1),
        eval_metric="logloss", random_state=42, verbosity=0,
    )
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
    prob = model.predict_proba(X_te)[:, 1]

    opt_thr, opt_f1 = _find_optimal_threshold(y_te.values, prob)
    print(f"\n[XGBoost 분류]  ROC-AUC {roc_auc_score(y_te, prob):.4f}  "
          f"최적 임계값: {opt_thr:.3f} (F1={opt_f1:.4f})")
    print(classification_report(y_te, (prob >= opt_thr).astype(int),
                                target_names=["정상", "부족"]))
    return model, prob, opt_thr

def save_charts(y_te, reg_pred, reg_model, cls_model, test_df):
    residuals = y_te.values - reg_pred

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("따릉이 수요예측 결과", fontsize=14, fontweight="bold")

    lim = max(y_te.max(), reg_pred.max()) * 1.05
    axes[0, 0].scatter(y_te, reg_pred, alpha=0.25, s=4, color="#0079F2", rasterized=True)
    axes[0, 0].plot([0, lim], [0, lim], "r--", lw=1)
    axes[0, 0].set(xlabel="실제 대여량", ylabel="예측 대여량", title="실제 vs 예측",
                   xlim=(0, lim), ylim=(0, lim))

    axes[0, 1].hist(residuals, bins=60, color="#795EFF", edgecolor="white", lw=0.2)
    axes[0, 1].axvline(0, color="red", ls="--", lw=1)
    axes[0, 1].set(xlabel="잔차 (실제 - 예측)", ylabel="빈도", title="잔차 분포")

    hour_mae = (pd.DataFrame({"hour": test_df["hour"].values, "mae": np.abs(residuals)})
                .groupby("hour")["mae"].mean())
    axes[1, 0].bar(hour_mae.index, hour_mae.values, color="#009118")
    axes[1, 0].set(xlabel="시간대", ylabel="MAE", title="시간대별 MAE")
    axes[1, 0].xaxis.set_major_locator(mticker.MultipleLocator(3))

    imp = pd.Series(reg_model.feature_importances_, index=FEATURE_COLS).sort_values(ascending=True).tail(12)
    imp.plot(kind="barh", ax=axes[1, 1], color="#0079F2")
    axes[1, 1].set(title="피처 중요도 (LightGBM)", xlabel="중요도")

    plt.tight_layout()
    plt.savefig(f"{OUT}/forecast_result.png"); plt.close()

    print(f"  저장: {OUT}/forecast_result.png")

def main():
    ts, log = make_log(OUT)
    print(f"=== 수요예측 모델  ({ts}) ===")
    engine = get_engine()

    print("데이터 로드 중...")
    X_tr, X_te, y_reg_tr, y_reg_te, y_cls_tr, y_cls_te, test_df, le = load_and_prepare(engine)
    print(f"  학습: {len(X_tr):,}  |  테스트: {len(X_te):,}")

    print("\n모델 학습 중...")
    reg_model, reg_pred   = train_regression(X_tr, y_reg_tr, X_te, y_reg_te)
    cls_model, _, opt_thr = train_classifier(X_tr, y_cls_tr, X_te, y_cls_te)

    print("\n차트 저장 중...")
    save_charts(y_reg_te, reg_pred, reg_model, cls_model, test_df)

    for name, obj in [("demand_reg", reg_model), ("shortage_cls", cls_model),
                      ("label_encoder", le), ("cls_threshold", opt_thr)]:
        with open(f"{MDL}/{name}.pkl", "wb") as f:
            pickle.dump(obj, f)
    print(f"  모델 저장: {MDL}/")
    print(f"\n로그: {OUT}/run_{ts}.txt")
    log.close()
    engine.dispose()

if __name__ == "__main__":
    main()
