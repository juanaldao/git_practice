# dags/postgres_to_duckdb.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import duckdb

def transfer_data_to_duckdb():
    """Extract data from PostgreSQL and load into DuckDB"""
    # Connect to PostgreSQL
    pg_engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    
    # Read data from PostgreSQL
    df = pd.read_sql("SELECT * FROM stock_prices", pg_engine)
    
    # Path where DuckDB file will be saved (inside the airflow container)
    duckdb_path = "/opt/airflow/dags/data/stock_prices.duckdb"
    
    # Connect to DuckDB and store the data in the volume
    conn = duckdb.connect(duckdb_path)
    
    # Create table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            Date DATE,
            Ticker VARCHAR,
            Closing_price DOUBLE
        )
    """)
    
    # Insert new data
    conn.register("df", df) # DuckDB doesnt know what df is unless you register it.
    conn.execute("INSERT INTO stock_prices SELECT * FROM df")
    
    # Commit and close connection
    conn.commit()
    conn.close()
    
    print(f"Successfully transferred {len(df)} records to DuckDB")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
#    'retry_delay': timedelta(minutes=5),
}

# DAG for transferring data from PostgreSQL to DuckDB
with DAG(
    'postgres_to_duckdb',
    default_args=default_args,
    description='Transfer stock data from PostgreSQL to DuckDB',
    schedule_interval=None,  # This DAG will be triggered by the first DAG
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=['stocks', 'duckdb'],
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_to_duckdb',
        python_callable=transfer_data_to_duckdb,
        dag=dag,
    )