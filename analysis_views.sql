SET NAMES utf8mb4;
SET time_zone = '+09:00';


-- 대여소 위험도 + 우선순위 점수
-- 부족/과잉 위험도: 빈도 60% + 강도 40% 가중합
-- 우선순위 점수: 위험도 60% + 최근 트렌드 25% + 지속성 15%

CREATE TABLE IF NOT EXISTS tb_station_risk (
    station_id               VARCHAR(20)   NOT NULL,
    avg_net_outflow_7d       DECIMAL(8,2)  COMMENT '최근 7일 일평균 순유출',
    shortage_days_7d         INT           COMMENT '최근 7일 부족 발생 일수',
    excess_days_7d           INT           COMMENT '최근 7일 과잉 발생 일수',
    avg_net_outflow_30d      DECIMAL(8,2)  COMMENT '최근 30일 일평균 순유출',
    shortage_days_30d        INT           COMMENT '최근 30일 부족 발생 일수',
    excess_days_30d          INT           COMMENT '최근 30일 과잉 발생 일수',
    shortage_risk            DECIMAL(5,3)  COMMENT '부족 위험도 0~1',
    excess_risk              DECIMAL(5,3)  COMMENT '과잉 위험도 0~1',
    supply_priority_score    DECIMAL(7,3)  COMMENT '보충 우선순위 0~100',
    retrieval_priority_score DECIMAL(7,3)  COMMENT '회수 우선순위 0~100',
    PRIMARY KEY (station_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='대여소별 위험도 (데이터 최대일 기준)';

TRUNCATE TABLE tb_station_risk;

SET @max_date = (SELECT MAX(record_date) FROM tb_daily_agg);
SET @date_7d  = DATE_SUB(@max_date, INTERVAL 6  DAY);
SET @date_30d = DATE_SUB(@max_date, INTERVAL 29 DAY);

INSERT INTO tb_station_risk (
    station_id,
    avg_net_outflow_7d,  shortage_days_7d,  excess_days_7d,
    avg_net_outflow_30d, shortage_days_30d, excess_days_30d,
    shortage_risk, excess_risk,
    supply_priority_score, retrieval_priority_score
)
WITH stats AS (
    SELECT
        station_id,
-- 7일 지표
        AVG(CASE WHEN record_date BETWEEN @date_7d AND @max_date
                 THEN daily_net_flow END)                                      AS avg_7d,
        SUM(CASE WHEN record_date BETWEEN @date_7d AND @max_date
                  AND daily_net_flow > 0 THEN 1 ELSE 0 END)                   AS s_days_7d,
        SUM(CASE WHEN record_date BETWEEN @date_7d AND @max_date
                  AND daily_net_flow < 0 THEN 1 ELSE 0 END)                   AS e_days_7d,
-- 30일 지표
        AVG(CASE WHEN record_date BETWEEN @date_30d AND @max_date
                 THEN daily_net_flow END)                                      AS avg_30d,
        SUM(CASE WHEN record_date BETWEEN @date_30d AND @max_date
                  AND daily_net_flow > 0 THEN 1 ELSE 0 END)                   AS s_days_30d,
        SUM(CASE WHEN record_date BETWEEN @date_30d AND @max_date
                  AND daily_net_flow < 0 THEN 1 ELSE 0 END)                   AS e_days_30d,
        SUM(CASE WHEN record_date BETWEEN @date_30d AND @max_date
                 THEN 1 ELSE 0 END)                                            AS total_30d
    FROM tb_daily_agg
    GROUP BY station_id
),
risks AS (
    SELECT *,
        ROUND(LEAST(1.0,
            (s_days_30d / NULLIF(total_30d, 0)) * 0.6
            + LEAST(1.0, GREATEST(0, avg_30d) / 50.0) * 0.4
        ), 3) AS shortage_risk,
        ROUND(LEAST(1.0,
            (e_days_30d / NULLIF(total_30d, 0)) * 0.6
            + LEAST(1.0, GREATEST(0, -avg_30d) / 50.0) * 0.4
        ), 3) AS excess_risk
    FROM stats
)
SELECT
    station_id,
    ROUND(avg_7d,  2), s_days_7d, e_days_7d,
    ROUND(avg_30d, 2), s_days_30d, e_days_30d,
    shortage_risk,
    excess_risk,
    IF(avg_30d > 0,
        ROUND(
            shortage_risk * 60.0
            + LEAST(1.0, GREATEST(0, avg_7d) / 50.0) * 25.0
            + (s_days_7d / 7.0) * 15.0,
        3), NULL),
    IF(avg_30d < 0,
        ROUND(
            excess_risk * 60.0
            + LEAST(1.0, GREATEST(0, -avg_7d) / 50.0) * 25.0
            + (e_days_7d / 7.0) * 15.0,
        3), NULL)
FROM risks;


-- 분석용 뷰

-- 보충 우선순위 랭킹
CREATE OR REPLACE VIEW v_supply_ranking AS
SELECT
    RANK() OVER (ORDER BY r.supply_priority_score DESC) AS supply_rank,
    m.station_id,
    m.station_name,
    m.district,
    m.total_slots,
    r.avg_net_outflow_7d,
    r.avg_net_outflow_30d,
    r.shortage_days_7d,
    r.shortage_days_30d,
    r.shortage_risk,
    r.supply_priority_score,
    CASE
        WHEN r.supply_priority_score >= 70 THEN '긴급'
        WHEN r.supply_priority_score >= 50 THEN '높음'
        WHEN r.supply_priority_score >= 30 THEN '보통'
        ELSE '낮음'
    END AS urgency,
    CONCAT('자전거 ', CEIL(r.avg_net_outflow_30d * 1.2), '대 보충') AS recommended_action
FROM tb_station_risk r
JOIN tb_station_master m ON r.station_id = m.station_id
WHERE r.supply_priority_score IS NOT NULL;

-- 회수 우선순위 랭킹
CREATE OR REPLACE VIEW v_retrieval_ranking AS
SELECT
    RANK() OVER (ORDER BY r.retrieval_priority_score DESC) AS retrieval_rank,
    m.station_id,
    m.station_name,
    m.district,
    m.total_slots,
    r.avg_net_outflow_7d,
    r.avg_net_outflow_30d,
    r.excess_days_7d,
    r.excess_days_30d,
    r.excess_risk,
    r.retrieval_priority_score,
    CASE
        WHEN r.retrieval_priority_score >= 70 THEN '긴급'
        WHEN r.retrieval_priority_score >= 50 THEN '높음'
        WHEN r.retrieval_priority_score >= 30 THEN '보통'
        ELSE '낮음'
    END AS urgency,
    CONCAT('자전거 ', CEIL(ABS(r.avg_net_outflow_30d) * 1.2), '대 회수') AS recommended_action
FROM tb_station_risk r
JOIN tb_station_master m ON r.station_id = m.station_id
WHERE r.retrieval_priority_score IS NOT NULL;

-- Python ML 입력용 피처 뷰
CREATE OR REPLACE VIEW v_features_for_ml AS
SELECT
    h.station_id,
    h.record_date,
    h.hour,
    h.day_of_week,
    h.is_weekend,
    h.rentals,
    h.returns,
    h.net_flow,
    d.ma_7d_rentals,
    d.ma_30d_rentals,
    d.ma_7d_netflow,
    d.ma_30d_netflow,
    r.shortage_risk,
    r.excess_risk,
    r.avg_net_outflow_7d,
    l.rentals_lag_24h,
    l.netflow_lag_24h,
    l.rentals_lag_1w,
    m.total_slots,
    m.district
FROM tb_hourly_agg  h
JOIN  tb_station_master m ON h.station_id = m.station_id
JOIN  tb_station_risk   r ON h.station_id = r.station_id
LEFT JOIN tb_daily_agg  d ON h.station_id = d.station_id  AND h.record_date = d.record_date
LEFT JOIN tb_hourly_lag l ON h.station_id = l.station_id
                         AND h.record_date = l.record_date
                         AND h.hour        = l.hour;


-- 결과 확인 쿼리

SELECT
    MIN(record_date)                                    AS 데이터_시작일,
    MAX(record_date)                                    AS 데이터_최대일,
    DATE_SUB(MAX(record_date), INTERVAL  6 DAY)         AS 최근7일_기준,
    DATE_SUB(MAX(record_date), INTERVAL 29 DAY)         AS 최근30일_기준,
    COUNT(DISTINCT station_id)                          AS 총_대여소,
    COUNT(DISTINCT record_date)                         AS 데이터_일수,
    FORMAT(SUM(rentals), 0)                             AS 총_대여건수
FROM tb_hourly_agg;

SELECT supply_rank, station_name, district, shortage_risk,
       ROUND(supply_priority_score, 1) AS 보충점수, urgency, recommended_action
FROM v_supply_ranking LIMIT 10;

SELECT retrieval_rank, station_name, district, excess_risk,
       ROUND(retrieval_priority_score, 1) AS 회수점수, urgency, recommended_action
FROM v_retrieval_ranking LIMIT 10;

SELECT
    hour,
    ROUND(AVG(rentals),  2) AS avg_rentals,
    ROUND(AVG(returns),  2) AS avg_returns,
    ROUND(AVG(net_flow), 2) AS avg_net_flow
FROM tb_hourly_agg
GROUP BY hour ORDER BY hour;

SELECT
    m.district,
    COUNT(*)                                               AS 대여소_수,
    SUM(IF(r.supply_priority_score    IS NOT NULL, 1, 0))  AS 부족_대여소,
    SUM(IF(r.retrieval_priority_score IS NOT NULL, 1, 0))  AS 과잉_대여소,
    ROUND(AVG(r.shortage_risk), 3)                         AS 평균_부족_위험도,
    ROUND(AVG(r.excess_risk),   3)                         AS 평균_과잉_위험도
FROM tb_station_risk r
JOIN tb_station_master m ON r.station_id = m.station_id
GROUP BY m.district
ORDER BY 평균_부족_위험도 DESC;
