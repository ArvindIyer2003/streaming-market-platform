# Real-Time Financial Analytics Platform

## Architecture
Data Sources → Kafka → Spark Streaming → Delta Lake → Snowflake → DBT → Streamlit/Tableau

## Stack
- **Ingestion:** Apache Kafka
- **Processing:** Spark Structured Streaming
- **Storage:** Delta Lake (Bronze/Silver/Gold) → Snowflake
- **Transformation:** DBT
- **Orchestration:** Apache Airflow
- **ML:** XGBoost (price direction prediction)
- **Dashboard:** Streamlit (real-time) + Tableau (analytics)

## Setup
1. Clone repo
2. Copy `.env.example` to `.env` and fill credentials
3. Run `docker-compose up -d`
4. Install dependencies: `pip install -r requirements.txt`
5. Run producer: `python producers/stock_producer.py`

## Project Structure
\`\`\`
streaming-market-platform/
├── producers/          # Kafka producers
├── spark_jobs/         # Spark streaming jobs
├── airflow/dags/       # Airflow DAGs
├── dbt/                # DBT transformations
├── ml/                 # ML training + inference
├── dashboard/          # Streamlit + Tableau
└── docker/             # Docker configs
\`\`\`

## Status
🔨 Week 1: Environment Setup - IN PROGRESS