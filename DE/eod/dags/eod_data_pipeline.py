# dags/eod_data_pipeline.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys
import os

# Add the directory containing EOD.py to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from EOD import run as eod_run

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
#    'retry_delay': timedelta(minutes=5),
}

# DAG for fetching EOD data and storing in PostgreSQL
with DAG(
    'fetch_eod_data',
    default_args=default_args,
    description='Fetch EOD stock data and store in PostgreSQL',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=['stocks', 'eod'],
) as dag:

    fetch_eod_data = PythonOperator(
        task_id='fetch_eod_data',
        python_callable=eod_run,
        dag=dag,
    )

    # Trigger the second DAG after EOD data is fetched
    trigger_duckdb_dag = TriggerDagRunOperator(
        task_id='trigger_duckdb_dag',
        trigger_dag_id='postgres_to_duckdb',
        wait_for_completion=True,
        reset_dag_run=True,
        dag=dag,
    )

    fetch_eod_data >> trigger_duckdb_dag