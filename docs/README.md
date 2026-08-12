# Bus Sensor Analytics Pipeline

An end-to-end ELT data pipeline that extracts IoT sensor telemetry from Oracle DB, 
orchestrates loads via Apache Airflow, lands date-partitioned Parquet files in Amazon S3, 
and auto-ingests into Snowflake via Snowpipe.

**Architecture:**
- **Extract:** Apache Airflow `PythonOperator` with `oracledb` + `pandas` + `pyarrow`
- **Load:** Snowpipe serverless ingestion from S3 to Snowflake raw tables
- **Transform:** dbt 3-layer architecture (raw → staging → marts) with star schema
- **Quality:** 21+ dbt tests (`not_null`, `unique`, `accepted_values`, `accepted_range`)
- **Serve:** Apache Superset dashboards via SQLAlchemy

**Tech Stack:** Oracle DB · Apache Airflow · Amazon S3 · Snowpipe · Snowflake · dbt · Apache Superset
