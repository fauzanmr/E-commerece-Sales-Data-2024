from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add your ETL script path so Python can import it
sys.path.insert(0, '/path/to/your/etl_script_directory') # dont need this if python file and dag file are in the same directory

from main import create_tables, extract_and_transform, create_views

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_etl_dag',
    default_args=default_args,
    description='ETL pipeline for e-commerce sales data',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 26),
    catchup=False,
    max_active_runs=1,
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )

    extract_transform_task = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform
    )

    create_views_task = PythonOperator(
        task_id='create_views',
        python_callable=create_views
    )

    # Define task dependencies
    create_tables_task >> extract_transform_task >> create_views_task
