# dags/eod_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import EOD  # This imports EOD.py

with DAG(
    dag_id="eod_stock_prices_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["stocks", "eod"],
) as dag:

    run_eod = PythonOperator(
        task_id="fetch_and_store_eod_data",
        python_callable=EOD.run
    )

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_duckdb = TriggerDagRunOperator(
    task_id="trigger_postgres_to_duckdb",
    trigger_dag_id="postgres_to_duckdb",  # ID of the DAG to trigger
)

# Add this to the task flow
run_eod >> trigger_duckdb
