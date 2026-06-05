# E-commerce Clickstream Analytics Pipeline

Spark 기반 대규모 클릭스트림 ETL 파이프라인 프로젝트

## 프로젝트 개요

사용자의 E-commerce 클릭 이벤트 데이터를 생성하고,

Bronze → Silver → Gold 아키텍처를 통해 정제/집계하여  
비즈니스 분석 지표를 생성하는 데이터 엔지니어링 프로젝트입니다.

Airflow를 활용해 전체 ETL 워크플로우를 자동화했습니다.

---

## Architecture

Generator
↓
Raw Clickstream CSV
↓
Bronze Layer (Raw Ingestion)
↓
Silver Layer (Data Cleansing)
↓
Gold Layer (Business Aggregation)
↓
Airflow Orchestration

---

## Tech Stack

- Python
- Apache Spark
- Apache Airflow
- Docker
- Pandas
- Parquet

---

## Directory Structure

project/
├── generator/
│   └── generate_clickstream.py
│
├── spark_jobs/
│   ├── bronze_job.py
│   ├── silver_job.py
│   └── gold_job.py
│
├── airflow/
│   └── dags/
│       └── clickstream_pipeline.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── docker-compose.yml

---

## ETL Flow

### 1. Bronze Layer

Raw clickstream ingestion

- CSV 수집
- Schema 적용
- Parquet 변환

### 2. Silver Layer

Data cleansing

- Null 제거
- Invalid event 제거
- Timestamp 정제

### 3. Gold Layer

Business metrics aggregation

- 페이지별 클릭 수
- 사용자 행동 분석
- 이벤트 통계 집계

---

## Workflow Orchestration

Airflow DAG를 사용해:

1. Clickstream 생성
2. Bronze 적재
3. Silver 정제
4. Gold 집계

전체 프로세스를 자동 실행

---

## Key Outcomes

- Spark ETL 파이프라인 구축
- Medallion Architecture 적용
- Airflow DAG orchestration
- Parquet 기반 최적화
- 데이터 분석 지표 생성

---

## What I Learned

- 대용량 데이터 처리
- Spark DataFrame API
- ETL 설계 패턴
- Workflow orchestration
- 데이터 레이어링 아키텍처
