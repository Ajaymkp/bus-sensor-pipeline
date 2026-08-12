
🚌 Bus Sensor Analytics Pipeline
End-to-end ELT pipeline extracting IoT sensor telemetry from Oracle DB → S3 → Snowflake → dbt → Apache Superset.

    Note: This project was built on Arch Linux + Hyprland with local Docker containers. The Airflow 3.3.0 scheduler has a known executor bug, so we use airflow tasks test for reliable execution.

🏗️ Architecture
plain

┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Oracle DB  │────▶│   Airflow   │────▶│  Amazon S3  │────▶│  Snowpipe   │
│  (Docker)   │     │   (local)   │     │  (Parquet)  │     │(auto-load)  │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                    │
┌─────────────┐     ┌─────────────┐     ┌─────────────┐            │
│  Superset   │◀────│    dbt      │◀────│  Snowflake  │◀───────────┘
│ (Dashboard) │     │(transform) │     │  (raw/marts)│
└─────────────┘     └─────────────┘     └─────────────┘

🚀 Quick Start
1. Start Oracle DB (Docker)
bash

docker start oracle

Verify it's running:
bash

docker ps | grep oracle

2. Start Airflow
bash

pkill -9 -f "airflow"
nohup airflow standalone > ~/airflow/standalone.log 2>&1 &
sleep 10
ss -tlnp | grep 8081

Check scheduler status:
bash

grep -i "scheduler" ~/airflow/standalone.log | tail -5

3. Run dbt Models
bash

source ~/.venvs/dbt/bin/activate
cd dbt
dbt run
dbt test

⚠️ Known Issue: Airflow 3.3.0 Scheduler Bug
Airflow 3.3.0's LocalExecutor crashes with an httpx pickling error when running tasks via the scheduler UI:
plain

TypeError: HTTPStatusError.__init__() missing 2 required keyword-only arguments: 'request' and 'response'

This causes tasks to stay stuck in "queued" state forever.
✅ Workaround: Use airflow tasks test
Run the pipeline end-to-end via CLI task testing (no scheduler needed):
bash

# Task 1: Extract Oracle → S3
airflow tasks test bus_censor_oracle_to_s3 extract_oracle_to_s3 2026-08-10

# Task 2: dbt run (build models)
airflow tasks test bus_censor_oracle_to_s3 dbt_run_models 2026-08-10

# Task 3: dbt test (validate data quality)
airflow tasks test bus_censor_oracle_to_s3 dbt_test_models 2026-08-10

    Why this works: airflow tasks test bypasses the scheduler/executor entirely and runs tasks in a local subprocess. It executes the exact same Python callable and Bash commands as the DAG would — just without the buggy queueing mechanism.

📁 Project Structure
plain

bus-censor-project/
├── dags/
│   └── bus_censor_oracle_to_s3.py      # Airflow DAG (extract + dbt run/test)
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── schema.yml              # 21+ dbt tests
│   │   │   └── stg_bus_sensors.sql     # Staging view
│   │   └── marts/
│   │       ├── dim_bus_status.sql      # Dimension table
│   │       ├── fct_sensor_readings.sql   # Fact table (aggregated)
│   │       ├── fct_sensor_locations.sql  # Fact table (geospatial)
│   │       └── schema.yml              # Mart model tests
│   ├── dbt_project.yml
│   └── packages.yml                    # dbt_utils dependency
├── .dbt/
│   └── profiles.yml                    # Snowflake connection (key-pair auth)
└── README.md

🛠️ Tech Stack
Table
Layer	Tool
Source DB	Oracle DB (Docker)
Orchestration	Apache Airflow 3.3.0
Object Storage	Amazon S3
Data Warehouse	Snowflake
Auto-Ingestion	Snowpipe
Transformation	dbt (data build tool)
Data Quality	dbt tests + dbt_utils
BI / Dashboards	Apache Superset
OS	Arch Linux + Hyprland
🔑 Key Features

    Date-partitioned S3 structure: year=2026/month=08/day=10/
    PyArrow schema enforcement: Prevents Snowflake timestamp corruption by writing recorded_at as ISO strings in Parquet
    3-layer dbt architecture: Raw → Staging → Marts (star schema)
    21+ automated data quality tests: not_null, unique, accepted_values, accepted_range
    Key-pair authentication: Snowflake connection via RSA private key (.p8)
    Real-time dashboards: Superset connected via SQLAlchemy to Snowflake marts

🐛 Troubleshooting Log
Table
Issue	Root Cause	Fix
Invalid date in Snowflake	Pandas datetime64[ns] → Parquet nanoseconds incompatible with Snowflake	Cast to datetime64[us] then force PyArrow pa.string() schema for recorded_at
dbt profile not found	File was in project .dbt/ instead of ~/.dbt/	cp .dbt/profiles.yml ~/.dbt/profiles.yml
source.raw.raw_bus_sensors duplicate	Source defined in both schema.yml and sources.yml	Removed sources: block from schema.yml
Snowflake connection timeout	Wrong account format (KR23933.GCP_EUROPE_WEST4)	Changed to KR23933.europe-west4.gcp
dbt_utils tests failing	dbt_utils package not installed	Added packages.yml + ran dbt deps
Airflow tasks stuck "queued"	Airflow 3.3.0 LocalExecutor + httpx pickling bug	Use airflow tasks test CLI instead of scheduler UI
📊 Dashboards (Superset)

    Daily Sensor Trends: Line chart — reading_date vs avg_value, grouped by sensor_type
    Warning Count: Bar chart — reading_date vs warning_count
    Bus Locations Map: deck.gl Scatterplot — lat/lon colored by status
    Bus Status Overview: Table view of dim_bus_status
