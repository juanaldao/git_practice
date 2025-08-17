# dags/postgres_to_duck.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import duckdb
from sqlalchemy import create_engine
import os

def transfer_to_duckdb():

    # Connect to PostgreSQL
    pg_engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    df = pd.read_sql("SELECT * FROM stock_prices", pg_engine)

    # Ensure the DuckDB directory exists with proper permissions
    #data_dir = "/opt/airflow/data" # Este lo saque xq no andaba y puse el de abajo.
    data_dir = "/tmp/stocks.duckdb"
    os.makedirs(data_dir, exist_ok=True)
    
    # Get current user and group IDs
    import os
    uid = os.getuid()
    gid = os.getgid()
    
    # Explicitly set permissions on the directory
    try:
        os.system(f"chmod 777 {data_dir}")
    except Exception as e:
        print(f"Warning: Failed to set permissions on data dir: {e}")
    
    # Connect to DuckDB with explicit read-write mode
    db_path = os.path.join(data_dir, "stock_data.duckdb")
    print(f"Connecting to DuckDB at {db_path} with UID={uid}, GID={gid}")
    
    try:
        con = duckdb.connect(db_path)
        
        # Register DataFrame
        con.register("df_view", df)

        # Create table if not exists, then append
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices AS SELECT * FROM df_view WHERE FALSE
        """)
        con.execute("""
            INSERT INTO stock_prices SELECT * FROM df_view
        """)

        con.close()
        print(f"Successfully transferred data to DuckDB at {db_path}")
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")
        # Try alternate approach with read_only=False
        try:
            print("Trying alternate approach with read_only=False")
            con = duckdb.connect(db_path, read_only=False)
            con.register("df_view", df)
            con.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices AS SELECT * FROM df_view WHERE FALSE
            """)
            con.execute("""
                INSERT INTO stock_prices SELECT * FROM df_view
            """)
            con.close()
            print(f"Alternate approach succeeded!")
        except Exception as e2:
            print(f"Alternate approach also failed: {e2}")
            raise e2


with DAG(
    dag_id="postgres_to_duckdb",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["example"],
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_to_duckdb",
        python_callable=transfer_to_duckdb,
    )