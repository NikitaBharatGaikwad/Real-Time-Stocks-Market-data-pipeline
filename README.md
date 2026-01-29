# Real-Time-Stocks-Market-data-pipeline
End-to-end real-time stock market data pipeline using Kafka, MinIO, Airflow, dbt, and Snowflake with bronze–silver–gold analytics modeling.

## Project Structure

```text
real-time-stocks-pipeline/
├── producer/                         # Kafka Producer (Finnhub API → Kafka)
│   └── producer.py
│
├── consumer/                         # Kafka Consumer (Kafka → MinIO)
│   └── consumer.py
│
├── airflow/
│   └── dags/
│       └── minio_to_snowflake.py     # Orchestration DAG
│
├── dbt_stocks/
│   └── models/
│       ├── bronze/                   # Raw structured data
│       │   ├── bronze_stg_stock_quotes.sql
│       │   └── sources.yml
│       │
│       ├── silver/                   # Cleaned & validated data
│       │   └── silver_clean_stock_quotes.sql
│       │
│       └── gold/                     # Analytics-ready data
│           ├── gold_candlestick.sql
│           ├── gold_kpi.sql
│           └── gold_treechart.sql
│
├── dashboards/                       # BI Layer (Gold Consumers)
│   ├── tableau/
│   │   └── stock_dashboard.twbx
│   └── powerbi/
│       └── stock_dashboard.pbix
│
├── ml/                               # ML on Gold Data
│   ├── feature_engineering.py
│   ├── train_model.py
│   ├── predict.py
│   └── model.pkl
│
├── docker-compose.yml                # Kafka, Zookeeper, MinIO, Airflow, Postgres
├── requirements.txt
└── README.md
