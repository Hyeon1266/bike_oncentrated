SET NAMES utf8mb4;
SET time_zone = '+09:00';


-- 원천 테이블 생성 및 데이터 적재

-- 대여소 마스터
CREATE TABLE IF NOT EXISTS tb_station_master (
    station_id   VARCHAR(20)   NOT NULL  COMMENT '대여소번호',
    station_name VARCHAR(100)  NOT NULL  COMMENT '대여소명',
    district     VARCHAR(30)             COMMENT '자치구',
    address      VARCHAR(200)            COMMENT '상세주소',
    lat          DECIMAL(10,6)           COMMENT '위도',
    lng          DECIMAL(10,6)           COMMENT '경도',
    total_slots  INT           NOT NULL DEFAULT 15 COMMENT '거치대수(LCD)',
    is_active    TINYINT(1)    NOT NULL DEFAULT 1  COMMENT '운영여부',
    PRIMARY KEY (station_id),
    INDEX idx_district (district)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='따릉이 대여소 마스터 (OA-21235)';

LOAD DATA LOCAL INFILE 'data/station_master.csv'
INTO TABLE tb_station_master
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(station_id, station_name, district, address, lat, lng, total_slots, @is_active)
SET is_active = IF(@is_active IN ('Y', '운영'), 1, 0);


-- 대여이력 원천
CREATE TABLE IF NOT EXISTS tb_rental_raw (
    id                  BIGINT       NOT NULL AUTO_INCREMENT,
    bike_no             VARCHAR(20)           COMMENT '자전거번호',
    rent_dt             DATETIME     NOT NULL  COMMENT '대여일시',
    rent_station_id     VARCHAR(20)  NOT NULL  COMMENT '대여 대여소번호',
    rent_station_name   VARCHAR(100)           COMMENT '대여 대여소명',
    return_dt           DATETIME              COMMENT '반납일시',
    return_station_id   VARCHAR(20)            COMMENT '반납대여소번호',
    return_station_name VARCHAR(100)           COMMENT '반납대여소명',
    use_min             INT                   COMMENT '이용시간(분)',
    use_dist_m          INT                   COMMENT '이용거리(M)',
    PRIMARY KEY (id),
    INDEX idx_rent   (rent_station_id,   rent_dt),
    INDEX idx_return (return_station_id, return_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='공공자전거 대여이력 원천 (OA-15182)';

-- 날짜 : 초 포함(HH:MM:SS) / 미포함(HH:MM) 자동 대응
LOAD DATA LOCAL INFILE 'data/rental_history.csv'
INTO TABLE tb_rental_raw
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(bike_no,
 @rent_dt,   rent_station_id,   rent_station_name,
 @return_dt, return_station_id, return_station_name,
 use_min, use_dist_m)
SET
    rent_dt   = COALESCE(
                    STR_TO_DATE(TRIM(@rent_dt), '%Y-%m-%d %H:%i:%s'),
                    STR_TO_DATE(TRIM(@rent_dt), '%Y-%m-%d %H:%i')
                ),
    return_dt = IF(TRIM(@return_dt) = '', NULL,
                    COALESCE(
                        STR_TO_DATE(TRIM(@return_dt), '%Y-%m-%d %H:%i:%s'),
                        STR_TO_DATE(TRIM(@return_dt), '%Y-%m-%d %H:%i')
                    ));


-- 시간대별 대여·반납 집계
CREATE TABLE IF NOT EXISTS tb_hourly_agg (
    station_id  VARCHAR(20) NOT NULL,
    record_date DATE        NOT NULL,
    hour        TINYINT     NOT NULL,
    day_of_week TINYINT     NOT NULL COMMENT '0=일, 6=토',
    is_weekend  TINYINT(1)  NOT NULL DEFAULT 0,
    rentals     INT         NOT NULL DEFAULT 0,
    returns     INT         NOT NULL DEFAULT 0,
    net_flow    INT         NOT NULL DEFAULT 0 COMMENT '순유출 = 대여 - 반납',
    PRIMARY KEY (station_id, record_date, hour),
    INDEX idx_date_hour   (record_date, hour),
    INDEX idx_station_dow (station_id,  day_of_week)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='대여소별 시간대별 대여·반납 통합';

TRUNCATE TABLE tb_hourly_agg;

INSERT INTO tb_hourly_agg
    (station_id, record_date, hour, day_of_week, is_weekend, rentals, returns, net_flow)
WITH events AS (

-- 대여
    SELECT rent_station_id             AS station_id,
           DATE(rent_dt)               AS record_date,
           HOUR(rent_dt)               AS hour,
           DAYOFWEEK(rent_dt) - 1     AS day_of_week,
           IF(DAYOFWEEK(rent_dt) IN (1, 7), 1, 0) AS is_weekend,
           1 AS is_rent, 0             AS is_return
    FROM tb_rental_raw
    WHERE rent_station_id IS NOT NULL AND rent_station_id <> ''

    UNION ALL

-- 반납
    SELECT return_station_id,
           DATE(return_dt),
           HOUR(return_dt),
           DAYOFWEEK(return_dt) - 1,
           IF(DAYOFWEEK(return_dt) IN (1, 7), 1, 0),
           0, 1
    FROM tb_rental_raw
    WHERE return_station_id IS NOT NULL
      AND return_station_id <> ''
      AND return_dt IS NOT NULL
)
SELECT
    station_id,
    record_date,
    hour,
    MAX(day_of_week) AS day_of_week,
    MAX(is_weekend)  AS is_weekend,
    SUM(is_rent)     AS rentals,
    SUM(is_return)   AS returns,
    SUM(is_rent) - SUM(is_return) AS net_flow
FROM events
GROUP BY station_id, record_date, hour
ON DUPLICATE KEY UPDATE
    rentals  = VALUES(rentals),
    returns  = VALUES(returns),
    net_flow = VALUES(net_flow);


-- 요일별 이용 패턴

CREATE TABLE IF NOT EXISTS tb_weekly_pattern (
    station_id   VARCHAR(20) NOT NULL,
    day_of_week  TINYINT     NOT NULL,
    is_weekend   TINYINT(1)  NOT NULL,
    avg_rentals  DECIMAL(8,2),
    avg_returns  DECIMAL(8,2),
    avg_net_flow DECIMAL(8,2),
    peak_hour    TINYINT COMMENT '대여 피크 시간',
    PRIMARY KEY (station_id, day_of_week)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='요일별 이용 패턴';

TRUNCATE TABLE tb_weekly_pattern;

INSERT INTO tb_weekly_pattern
    (station_id, day_of_week, is_weekend, avg_rentals, avg_returns, avg_net_flow, peak_hour)
SELECT
    station_id,
    day_of_week,
    is_weekend,
    ROUND(AVG(rentals),  2),
    ROUND(AVG(returns),  2),
    ROUND(AVG(net_flow), 2),
    SUBSTRING_INDEX(GROUP_CONCAT(hour ORDER BY rentals DESC SEPARATOR ','), ',', 1) + 0
FROM tb_hourly_agg
GROUP BY station_id, day_of_week, is_weekend;


-- 일별 집계 + 이동평균 (MySQL 8.0+ 윈도우 함수 사용)

CREATE TABLE IF NOT EXISTS tb_daily_agg (
    station_id     VARCHAR(20)   NOT NULL,
    record_date    DATE          NOT NULL,
    day_of_week    TINYINT       NOT NULL,
    is_weekend     TINYINT(1)    NOT NULL,
    daily_rentals  INT           NOT NULL DEFAULT 0,
    daily_returns  INT           NOT NULL DEFAULT 0,
    daily_net_flow INT           NOT NULL DEFAULT 0,
    ma_7d_rentals  DECIMAL(10,2) COMMENT '직전 7일 이동평균 대여량',
    ma_30d_rentals DECIMAL(10,2) COMMENT '직전 30일 이동평균 대여량',
    ma_7d_netflow  DECIMAL(10,2) COMMENT '직전 7일 이동평균 순유출',
    ma_30d_netflow DECIMAL(10,2) COMMENT '직전 30일 이동평균 순유출',
    PRIMARY KEY (station_id, record_date),
    INDEX idx_date (record_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='일별 집계 + 이동평균';

TRUNCATE TABLE tb_daily_agg;

INSERT INTO tb_daily_agg
    (station_id, record_date, day_of_week, is_weekend,
     daily_rentals, daily_returns, daily_net_flow,
     ma_7d_rentals, ma_30d_rentals, ma_7d_netflow, ma_30d_netflow)
WITH daily AS (
    SELECT
        station_id,
        record_date,
        MAX(day_of_week) AS day_of_week,
        MAX(is_weekend)  AS is_weekend,
        SUM(rentals)     AS daily_rentals,
        SUM(returns)     AS daily_returns,
        SUM(net_flow)    AS daily_net_flow
    FROM tb_hourly_agg
    GROUP BY station_id, record_date
)
SELECT
    station_id, record_date, day_of_week, is_weekend,
    daily_rentals, daily_returns, daily_net_flow,
    ROUND(AVG(daily_rentals)  OVER w7,  2),
    ROUND(AVG(daily_rentals)  OVER w30, 2),
    ROUND(AVG(daily_net_flow) OVER w7,  2),
    ROUND(AVG(daily_net_flow) OVER w30, 2)
FROM daily
WINDOW
    w7  AS (PARTITION BY station_id ORDER BY record_date ROWS BETWEEN  6 PRECEDING AND CURRENT ROW),
    w30 AS (PARTITION BY station_id ORDER BY record_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW);


-- LAG 피처 사전 계산 (조회 시 반복 계산 방지)

CREATE TABLE IF NOT EXISTS tb_hourly_lag (
    station_id      VARCHAR(20) NOT NULL,
    record_date     DATE        NOT NULL,
    hour            TINYINT     NOT NULL,
    rentals_lag_24h INT         COMMENT '24시간 전 대여량',
    netflow_lag_24h INT         COMMENT '24시간 전 순유출',
    rentals_lag_1w  INT         COMMENT '1주일 전 대여량',
    PRIMARY KEY (station_id, record_date, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='사전 계산된 LAG 피처 (tb_hourly_agg 기반)';

TRUNCATE TABLE tb_hourly_lag;

INSERT INTO tb_hourly_lag
    (station_id, record_date, hour, rentals_lag_24h, netflow_lag_24h, rentals_lag_1w)
SELECT
    station_id,
    record_date,
    hour,
    LAG(rentals,  24)  OVER (PARTITION BY station_id ORDER BY record_date, hour),
    LAG(net_flow, 24)  OVER (PARTITION BY station_id ORDER BY record_date, hour),
    LAG(rentals,  168) OVER (PARTITION BY station_id ORDER BY record_date, hour)
FROM tb_hourly_agg;

-- 통계 갱신 (옵티마이저 플랜 정확 향상)
ANALYZE TABLE tb_hourly_agg, tb_daily_agg, tb_hourly_lag, tb_station_master;
