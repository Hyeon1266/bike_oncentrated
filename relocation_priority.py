import os
from collections import Counter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import text

from utils import init, make_log, get_engine
init()

OUT = "outputs/relocation"
os.makedirs(OUT, exist_ok=True)

RISK_COLOR = {"부족": "#A60808", "과잉": "#F59E0B"}

# 재배치 1회 시행 후 우선순위 점수 잔존 비율 가정값 — 실제 검증 안 됨
SUPPLY_REDUCE    = 0.45   # 보충 후 55% 감소 가정 (0.3도 해봤는데 너무 보수적)
RETRIEVAL_REDUCE = 0.50   # 회수 후 50% 감소 가정

def load_all(engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with engine.connect() as conn:
        supply = pd.read_sql("""
            SELECT s.*, COALESCE(c.cluster_label, '미분류') AS cluster_label
            FROM v_supply_ranking s
            LEFT JOIN tb_station_cluster c USING (station_id)
        """, conn)
        supply["risk_type"] = "부족"

        retrieval = pd.read_sql("""
            SELECT r.*, COALESCE(c.cluster_label, '미분류') AS cluster_label
            FROM v_retrieval_ranking r
            LEFT JOIN tb_station_cluster c USING (station_id)
        """, conn)
        retrieval["risk_type"] = "과잉"

        hourly = pd.read_sql("""
            SELECT station_id, hour,
                   AVG(net_flow) AS avg_net_flow
            FROM tb_hourly_agg
            GROUP BY station_id, hour
        """, conn)

    return supply, retrieval, hourly

def simulate_before_after(supply: pd.DataFrame, retrieval: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    s = supply.head(top_n)[["station_name", "supply_priority_score"]].copy()
    s["risk_type"] = "부족"
    s = s.rename(columns={"supply_priority_score": "before"})
    s["after"] = (s["before"] * SUPPLY_REDUCE).round(2)

    r = retrieval.head(top_n)[["station_name", "retrieval_priority_score"]].copy()
    r["risk_type"] = "과잉"
    r = r.rename(columns={"retrieval_priority_score": "before"})
    r["after"] = (r["before"] * RETRIEVAL_REDUCE).round(2)

    sim = pd.concat([s, r]).sort_values("before", ascending=False).head(top_n).reset_index(drop=True)
    sim["improvement_pct"] = ((sim["before"] - sim["after"]) / sim["before"].replace(0, np.nan) * 100)\
                             .fillna(0).round(1)
    return sim

def _hbar(ax, names, scores, colors, score_label, title):
    names_r  = names[::-1]
    scores_r = scores[::-1]
    colors_r = colors[::-1]
    bars = ax.barh(names_r, scores_r, color=colors_r, edgecolor="white", lw=0.4)
    for bar, score in zip(bars, scores_r):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{score:.1f}", va="center", fontsize=8)
    ax.set(xlabel=score_label, title=title,
           xlim=(0, max(scores) * 1.15 if len(scores) else 1))
    ax.grid(axis="x", alpha=0.3)


def save_charts(supply, retrieval, hourly, sim, top_n=20):
    # 히트맵 인덱스 레이블용 대여소명 매핑
    name_map = (pd.concat([supply[["station_id", "station_name"]],
                            retrieval[["station_id", "station_name"]]])
                  .drop_duplicates("station_id")
                  .set_index("station_id")["station_name"])

    def _unique_labels(sids: list, nmap: pd.Series, max_len: int = 12) -> list:
        """대여소명 앞 max_len 자 레이블. 중복 시 ID 뒤 4자리 추가."""
        raw    = [nmap.get(sid, sid)[:max_len] for sid in sids]
        counts = Counter(raw)
        return [
            f"{lbl[:max_len-5]}({str(sid)[-4:]})" if counts[lbl] > 1 else lbl
            for sid, lbl in zip(sids, raw)
        ]

    # 보충 우선순위 랭킹
    top_s = supply.head(top_n)
    fig, ax = plt.subplots(figsize=(10, 8))
    _hbar(ax, top_s["station_name"].tolist(),
          top_s["supply_priority_score"].tolist(),
          ["#A60808"] * top_n,
          "보충 우선순위 점수 (참고용)", f"자전거 보충 필요 대여소 Top {top_n}")
    plt.tight_layout()
    plt.savefig(f"{OUT}/supply_ranking.png"); plt.close()

    # 회수 우선순위 랭킹
    top_r = retrieval.head(top_n)
    fig, ax = plt.subplots(figsize=(10, 8))
    _hbar(ax, top_r["station_name"].tolist(),
          top_r["retrieval_priority_score"].tolist(),
          ["#F59E0B"] * top_n,
          "회수 우선순위 점수 (참고용)", f"자전거 회수 필요 대여소 Top {top_n}")
    plt.tight_layout()
    plt.savefig(f"{OUT}/retrieval_ranking.png"); plt.close()

    # 재배치 전/후 비교
    names = [n[:12] for n in sim["station_name"]]
    x, w  = np.arange(len(names)), 0.35
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x - w/2, sim["before"], w, label="재배치 전",
           color=sim["risk_type"].map(RISK_COLOR).values, alpha=0.85)
    ax.bar(x + w/2, sim["after"],  w, label="재배치 후 (가정)", color="#009118", alpha=0.85)
    for xi, after, pct in zip(x, sim["after"], sim["improvement_pct"]):
        ax.text(xi + w/2, after + 0.3, f"-{pct}%",
                ha="center", va="bottom", fontsize=7, color="#009118")
    ax.set_xticks(x); ax.set_xticklabels(names, rotation=45, ha="right", fontsize=8)
    ax.set(ylabel="우선순위 점수",
           title=f"재배치 전/후 우선순위 점수 비교\n"
                 f"(가정: 보충 {int((1-SUPPLY_REDUCE)*100)}% 감소 / 회수 {int((1-RETRIEVAL_REDUCE)*100)}% 감소)")
    ax.legend(); ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{OUT}/before_after.png"); plt.close()

    # 부족/과잉 위험도 산점도
    s = supply[["shortage_risk", "excess_risk", "supply_priority_score"]]\
        .rename(columns={"supply_priority_score": "score"}).assign(risk_type="부족")
    r = retrieval[["shortage_risk", "excess_risk", "retrieval_priority_score"]]\
        .rename(columns={"retrieval_priority_score": "score"}).assign(risk_type="과잉")
    all_df = pd.concat([s, r], ignore_index=True)
    colors = all_df["risk_type"].map(RISK_COLOR)
    size   = (all_df["score"] / all_df["score"].max() * 200).clip(10)
    fig, ax = plt.subplots(figsize=(9, 7))
    ax.scatter(all_df["shortage_risk"], all_df["excess_risk"],
               c=colors, s=size, alpha=0.6, edgecolors="white", lw=0.3)
    ax.axhline(0.5, color="gray", ls="--", lw=0.8, alpha=0.5)
    ax.axvline(0.5, color="gray", ls="--", lw=0.8, alpha=0.5)
    ax.text(0.72, 0.02, "부족 고위험", color="#A60808", fontsize=9)
    ax.text(0.02, 0.74, "과잉 고위험", color="#F59E0B", fontsize=9)
    ax.legend(handles=[plt.scatter([], [], c=c, s=50, label=l)
                       for l, c in [("부족", "#A60808"), ("과잉", "#F59E0B")]])
    ax.set(xlabel="부족 위험도", ylabel="과잉 위험도",
           title="대여소별 부족/과잉 위험도 분포", xlim=(0, 1), ylim=(0, 1))
    ax.grid(alpha=0.2); plt.tight_layout()
    plt.savefig(f"{OUT}/risk_scatter.png"); plt.close()

    # 자치구별 위험도 히트맵
    s_agg = supply.groupby("district")[["shortage_risk", "supply_priority_score"]].mean()
    r_agg = retrieval.groupby("district")[["excess_risk", "retrieval_priority_score"]].mean()
    pivot = s_agg.join(r_agg, how="outer").fillna(0)
    pivot.columns = ["부족 위험도", "보충 점수", "과잉 위험도", "회수 점수"]
    pivot = pivot.sort_values("보충 점수", ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(9, 9))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap="YlOrRd", linewidths=0.5, ax=ax)
    ax.set(title="자치구별 위험도 히트맵", xlabel="")
    plt.tight_layout()
    plt.savefig(f"{OUT}/district_heatmap.png"); plt.close()

    print(f"  저장 완료 ({OUT}/):")
    for f in ["supply_ranking", "retrieval_ranking", "before_after",
              "risk_scatter", "district_heatmap"]:
        print(f"    {f}.png")

def save_to_db(supply: pd.DataFrame, retrieval: pd.DataFrame, engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tb_supply_priority (
                station_id             VARCHAR(20)  NOT NULL PRIMARY KEY,
                shortage_risk          DECIMAL(5,3),
                supply_priority_score  DECIMAL(7,3),
                urgency                VARCHAR(10),
                recommended_action     VARCHAR(100)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tb_retrieval_priority (
                station_id               VARCHAR(20)  NOT NULL PRIMARY KEY,
                excess_risk              DECIMAL(5,3),
                retrieval_priority_score DECIMAL(7,3),
                urgency                  VARCHAR(10),
                recommended_action       VARCHAR(100)
            )
        """))
        conn.execute(text("TRUNCATE TABLE tb_supply_priority"))
        conn.execute(text("TRUNCATE TABLE tb_retrieval_priority"))
        conn.commit()

    supply[["station_id", "shortage_risk", "supply_priority_score",
            "urgency", "recommended_action"]]\
        .to_sql("tb_supply_priority", engine, if_exists="append", index=False)
    retrieval[["station_id", "excess_risk", "retrieval_priority_score",
               "urgency", "recommended_action"]]\
        .to_sql("tb_retrieval_priority", engine, if_exists="append", index=False)
    print(f"  DB 저장: tb_supply_priority ({len(supply)}) / tb_retrieval_priority ({len(retrieval)})")

# 메인

def main():
    ts, log = make_log(OUT)
    print(f"=== 재배치 우선순위 분석  ({ts}) ===")
    engine = get_engine()

    print("데이터 로드 중...")
    supply, retrieval, hourly = load_all(engine)
    print(f"  부족 대여소: {len(supply):,}  |  과잉 대여소: {len(retrieval):,}")

    # 점수 내림차순 정렬
    supply    = supply.sort_values("supply_priority_score",       ascending=False).reset_index(drop=True)
    retrieval = retrieval.sort_values("retrieval_priority_score", ascending=False).reset_index(drop=True)

    print("\n재배치 전/후 시뮬레이션...")
    print(f"  [가정] 보충 {int((1-SUPPLY_REDUCE)*100)}% 감소 / "
          f"회수 {int((1-RETRIEVAL_REDUCE)*100)}% 감소")
    sim = simulate_before_after(supply, retrieval, top_n=15)

    print("\n차트 저장 중...")
    save_charts(supply, retrieval, hourly, sim, top_n=20)

    print("\nDB 저장 중...")
    save_to_db(supply, retrieval, engine)

    supply.to_csv(f"{OUT}/supply_priority.csv",       index=False, encoding="utf-8-sig")
    retrieval.to_csv(f"{OUT}/retrieval_priority.csv", index=False, encoding="utf-8-sig")
    print(f"  CSV 저장: {OUT}/supply_priority.csv")
    print(f"  CSV 저장: {OUT}/retrieval_priority.csv")

    print(f"\n보충 {len(supply):,}개 / 회수 {len(retrieval):,}개  (긴급: {(supply['urgency']=='긴급').sum() + (retrieval['urgency']=='긴급').sum()}개)")
    print(f"로그: {OUT}/run_{ts}.txt")
    log.close()
    engine.dispose()


if __name__ == "__main__":
    main()
