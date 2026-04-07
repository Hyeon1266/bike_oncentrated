# 군집명은 중심값 기반 운영 관점 해석 — 확정 분류 아님

import os
from collections import Counter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sqlalchemy import text

from utils import init, make_log, get_engine
init()

OUT = "outputs/clustering"
os.makedirs(OUT, exist_ok=True)

LABEL_KO = {
    "commuter_hub": "출근형",
    "residential":  "주거형",
    "leisure":      "관광형",
    "office":       "업무형",
    "transit":      "혼합형",
}
COLOR = {
    "commuter_hub": "#0079F2",
    "residential":  "#009118",
    "leisure":      "#ec4899",
    "office":       "#795EFF",
    "transit":      "#F59E0B",
}

def build_features(engine) -> pd.DataFrame:
    sql = """
        SELECT station_id, hour, is_weekend,
               AVG(rentals)  AS avg_rentals,
               AVG(net_flow) AS avg_net_flow
        FROM tb_hourly_agg
        GROUP BY station_id, hour, is_weekend
    """
    df = pd.read_sql(sql, engine)

    # 평일/주말 시간대별 대여 피벗
    pv = df.pivot_table(index="station_id", columns=["is_weekend", "hour"],
                        values="avg_rentals", aggfunc="mean").fillna(0)
    pv.columns = [f"{'we' if w else 'wd'}_h{h}" for w, h in pv.columns]

    # 전체,평일,주말 순유출
    net    = df.groupby("station_id")["avg_net_flow"].mean().rename("avg_net_flow")
    net_wd = (df[df["is_weekend"] == 0]
              .groupby("station_id")["avg_net_flow"].mean()
              .rename("avg_net_flow_wd"))
    net_we = (df[df["is_weekend"] == 1]
              .groupby("station_id")["avg_net_flow"].mean()
              .rename("avg_net_flow_we"))
    feat = pv.join(net).join(net_wd).join(net_we)

    # 평일 기준 피크 비율
    wd_cols = [c for c in feat.columns if c.startswith("wd_h")]
    am_cols = [c for c in wd_cols if int(c.split("_h")[-1]) in range(7, 10)]
    pm_cols = [c for c in wd_cols if int(c.split("_h")[-1]) in range(17, 20)]
    total   = feat[wd_cols].sum(axis=1).replace(0, np.nan)
    feat["am_peak_ratio"] = feat[am_cols].sum(axis=1) / total
    feat["pm_peak_ratio"] = feat[pm_cols].sum(axis=1) / total

    return feat.fillna(0)

def find_optimal_k(X: np.ndarray, k_range=range(2, 8)) -> int:
    ks = list(k_range)
    inertias, silhouettes = [], []
    for k in ks:
        km = KMeans(n_clusters=k, random_state=42, n_init=5).fit(X)
        inertias.append(km.inertia_)
        silhouettes.append(silhouette_score(X, km.labels_))

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    axes[0].plot(ks, inertias,    "bo-"); axes[0].set(xlabel="K", ylabel="Inertia",          title="Elbow")
    axes[1].plot(ks, silhouettes, "rs-"); axes[1].set(xlabel="K", ylabel="Silhouette Score", title="Silhouette")
    plt.tight_layout()
    plt.savefig(f"{OUT}/k_selection.png"); plt.close()

    best_k = ks[int(np.argmax(silhouettes))]
    print(f"  최적 K = {best_k}  (Silhouette: {max(silhouettes):.4f})")
    # 처음엔 k=5 고정으로 했는데 데이터 따라 달라져서 자동 탐색으로 바꿈
    return best_k

def cluster_and_label(X: np.ndarray, k: int, feat: pd.DataFrame) -> pd.DataFrame:
    km = KMeans(n_clusters=k, random_state=42, n_init=20)
    feat = feat.copy()
    feat["cluster"] = km.fit_predict(X)
    print(f"  Silhouette: {silhouette_score(X, feat['cluster']):.4f}")

    # 피크 비율 임계값 3/24 ≈ 0.125
    profile = feat.groupby("cluster")[["am_peak_ratio", "pm_peak_ratio", "avg_net_flow"]].mean()
    print("\n  [군집 중심값 — 레이블링 근거]")
    print(profile.round(4).to_string())

    label_map = {}
    for c, (am, pm, nf) in profile.iterrows():
        if   am > 0.12 and nf >  1:   label_map[c] = "commuter_hub"
        elif pm > 0.12 and nf < -1:   label_map[c] = "residential"
        elif am < 0.08 and pm < 0.08: label_map[c] = "leisure"
        elif am > 0.10 and pm > 0.10: label_map[c] = "office"
        else:                          label_map[c] = "transit"

    feat["cluster_type"]  = feat["cluster"].map(label_map)
    feat["cluster_label"] = feat["cluster_type"].map(LABEL_KO)
    return feat

def save_charts(X: np.ndarray, feat: pd.DataFrame):
    wd_cols = sorted([c for c in feat.columns if c.startswith("wd_h")],
                     key=lambda c: int(c.split("_h")[-1]))
    hours = [int(c.split("_h")[-1]) for c in wd_cols]

    # 24시간 대여 프로파일
    cluster_means = {
        ct: feat.loc[feat["cluster_type"] == ct, wd_cols].values.mean(axis=0)
        for ct in COLOR if (feat["cluster_type"] == ct).any()
    }
    fig, ax = plt.subplots(figsize=(12, 5))
    for ct, mean_vals in cluster_means.items():
        ax.plot(hours, mean_vals, label=LABEL_KO[ct], color=COLOR[ct], lw=2.5)
    ax.set(xlabel="시간 (시)", ylabel="평균 대여량",
           title="군집별 평일 24시간 대여 프로파일 (참고용 운영 해석 유형)")
    ax.set_xticks(range(0, 24, 2)); ax.legend(); ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{OUT}/cluster_profiles.png"); plt.close()

    # PCA 2D 시각화
    pca    = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(X)
    ev     = pca.explained_variance_ratio_
    fig, ax = plt.subplots(figsize=(10, 7))
    for ct, color in COLOR.items():
        mask = feat["cluster_type"] == ct
        ax.scatter(coords[mask, 0], coords[mask, 1], c=color, label=LABEL_KO[ct],
                   alpha=0.7, s=35, edgecolors="white", lw=0.3)
    ax.set(xlabel=f"PC1 ({ev[0]*100:.1f}%)", ylabel=f"PC2 ({ev[1]*100:.1f}%)",
           title="대여소 군집화 결과 (PCA 2D)")
    ax.legend(); ax.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(f"{OUT}/cluster_pca.png"); plt.close()

    # 핵심 지표 히트맵
    summary = (feat.groupby("cluster_label")[["am_peak_ratio", "pm_peak_ratio", "avg_net_flow"]]
               .mean().rename(columns={"am_peak_ratio": "아침 피크",
                                       "pm_peak_ratio": "저녁 피크",
                                       "avg_net_flow":  "평균 순유출"}))
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.heatmap(summary, annot=True, fmt=".3f", cmap="RdYlGn_r", linewidths=0.5, ax=ax)
    ax.set(title="군집별 핵심 지표 히트맵", xlabel="")
    plt.tight_layout()
    plt.savefig(f"{OUT}/cluster_heatmap.png"); plt.close()

    print(f"  저장: {OUT}/k_selection.png")
    print(f"  저장: {OUT}/cluster_profiles.png")
    print(f"  저장: {OUT}/cluster_pca.png")
    print(f"  저장: {OUT}/cluster_heatmap.png")

def save_clusters(feat: pd.DataFrame, engine):
    result = feat[["cluster_type", "cluster_label"]].reset_index()
    result.columns = ["station_id", "cluster_type", "cluster_label"]

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tb_station_cluster (
                station_id    VARCHAR(20) NOT NULL PRIMARY KEY,
                cluster_type  VARCHAR(30),
                cluster_label VARCHAR(30),
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("TRUNCATE TABLE tb_station_cluster"))
        conn.commit()

    result.to_sql("tb_station_cluster", engine, if_exists="append", index=False)
    print(f"  DB 저장: tb_station_cluster ({len(result)}개 대여소)")

def main():
    ts, log = make_log(OUT)
    print(f"=== 대여소 유형 군집화  ({ts}) ===")
    engine = get_engine()

    print("피처 행렬 구성 중...")
    feat = build_features(engine)
    print(f"  대여소: {len(feat):,}  |  피처: {feat.shape[1]}")

    cluster_cols = feat.columns.tolist()
    X = StandardScaler().fit_transform(feat[cluster_cols])

    print("\n최적 K 탐색 중...")
    k = find_optimal_k(X)

    print(f"\nK-Means (k={k}) 군집화 중...")
    feat = cluster_and_label(X, k, feat)

    print("\n군집별 분포:")
    print(feat.groupby(["cluster_type", "cluster_label"]).size()
              .reset_index(name="count").to_string(index=False))

    print("\n차트 저장 중...")
    save_charts(X, feat)

    print("\nDB 저장 중...")
    save_clusters(feat, engine)

    out_csv = f"{OUT}/cluster_result.csv"
    feat[["cluster_type", "cluster_label", "am_peak_ratio", "pm_peak_ratio", "avg_net_flow"]]\
        .to_csv(out_csv, encoding="utf-8-sig")
    print(f"  CSV 저장: {out_csv}")
    print(f"\n로그: {OUT}/run_{ts}.txt")
    log.close()
    engine.dispose()


if __name__ == "__main__":
    main()
