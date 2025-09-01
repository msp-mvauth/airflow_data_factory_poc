from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import duckdb
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='example_load_to_motherduck',
    default_args=default_args,
    description='Load data into MotherDuck using DuckDB Python library',
    schedule=timedelta(minutes=1),
    catchup=False,
    tags=['example', 'motherduck'],
) as dag:

    @task
    def load_to_motherduck():
        md_token = os.environ.get("MOTHERDUCK_TOKEN")
        if not md_token:
            raise ValueError("MotherDuck token not set in environment variable MOTHERDUCK_TOKEN")

        conn = duckdb.connect(f'md:airflow_test?motherduck_token={md_token}')
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER,
                name VARCHAR
            );
        """)
        conn.execute("""
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
        """)
        conn.close()

    load_to_motherduck()
