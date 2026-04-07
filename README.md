# 서울시 따릉이 수요예측 & 재배치 우선순위 분석

따릉이 대여이력 데이터로 수요 예측, 대여소 군집화, 재배치 우선순위 점수를 산출하는 프로젝트입니다.
핵심 목표는 재배치 우선순위 산정이고, 수요 예측과 군집화는 이를 보조하기 위한 단계입니다.

## 사용 기술

주요: pandas, MySQL, LightGBM  
보조: XGBoost, scikit-learn, seaborn, matplotlib

## 실행 방법

```bash
pip install pandas numpy scikit-learn xgboost lightgbm sqlalchemy pymysql matplotlib seaborn
export MYSQL_URL="mysql+pymysql://user:password@host:3306/bike_db"
```

```bash
# 내 환경 기준 (MySQL 8.0, LOCAL INFILE 허용 필요)
mysql -u <user> -p <db> < preprocess.sql
mysql -u <user> -p <db> < analysis_views.sql

python demand_forecast.py
python clustering.py
python relocation_priority.py
```

실행하면 `outputs/` 폴더에 결과 파일과 그래프가 저장됩니다.

## 데이터

- [공공자전거 대여이력](https://data.seoul.go.kr/dataList/OA-15182/F/1/datasetView.do) → `data/rental_history.csv`
- [공공자전거 대여소 정보](https://data.seoul.go.kr/dataList/OA-21235/S/1/datasetView.do) → `data/station_master.csv`

## 시행착오

- 처음엔 수요예측 모델만 만들 생각이었는데 예측값만으로 어느 대여소에 자전거를 넣어야 할지 결정하기 애매해서 위험도 점수를 따로 만들게 됐습니다.
- 군집 레이블(출근형, 주거형 등)은 중심값 보고 직접 규칙 짠 건데 데이터 기간이나 계절에 따라 결과가 달라질 수 있어서 그대로 믿으면 안 됩니다.
- 재배치 시뮬레이션 감소율은 그냥 적당히 잡은 값입니다. 실제 운영하면 전혀 다를 수 있습니다.
