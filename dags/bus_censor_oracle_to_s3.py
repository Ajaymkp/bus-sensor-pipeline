from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import oracledb
import io

ORACLE_HOST, ORACLE_PORT, ORACLE_USER, ORACLE_PASS, ORACLE_SERVICE = "localhost", 1521, "SYSTEM", "admin", "XE"
S3_BUCKET, S3_PREFIX = "mkp-s3-data", "bus-censor/raw"
DBT_PROJECT_DIR = "/home/void-god/Ajay Mkp/A projects/Learning/Snow_flake/projects_&_assessments/bus-censor-project/dbt"

default_args = {
    "owner": "void-god",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def extract_oracle_to_s3(**context):
    execution_date = context["ds"]
    conn = oracledb.connect(
        user=ORACLE_USER, 
        password=ORACLE_PASS, 
        dsn=oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    )
    df = pd.read_sql(
        f"SELECT * FROM bus_sensors WHERE TRUNC(recorded_at) <= TO_DATE('{execution_date}', 'YYYY-MM-DD')", 
        conn
    )
    conn.close()
    
    if df.empty:
        return f"s3://{S3_BUCKET}/{S3_PREFIX}/{execution_date}/no_data.flag"
    
    df.columns = [c.lower() for c in df.columns]
    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    
    s3_key = (
        f"{S3_PREFIX}/year={execution_date[:4]}/"
        f"month={execution_date[5:7]}/"
        f"day={execution_date[8:10]}/"
        f"bus_sensors_{execution_date}.parquet"
    )
    
    S3Hook(aws_conn_id="aws_default").load_bytes(
        bytes_data=buffer.getvalue(), 
        key=s3_key, 
        bucket_name=S3_BUCKET, 
        replace=True
    )
    print(f"Uploaded {len(df)} rows to s3://{S3_BUCKET}/{s3_key}")
    return f"s3://{S3_BUCKET}/{s3_key}"

with DAG(
    "bus_censor_oracle_to_s3",
    default_args=default_args,
    description="Oracle → S3 → dbt → Snowflake",
    schedule="@daily",
    start_date=datetime(2026, 8, 6),
    catchup=False,
    tags=["bus", "oracle", "s3", "dbt", "snowflake"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_oracle_to_s3",
        python_callable=extract_oracle_to_s3,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt run',
    )

    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt test',
    )

    # Pipeline flow
    extract_task >> dbt_run >> dbt_test