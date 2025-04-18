# Standard library imports for file/system operations and time handling
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Airflow imports for DAG creation and task operators
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Add src directory to Python path for custom module imports
sys.path.append('../src')
import extract
import load
from create_data_dir import create_data_directories


# DAG configuration parameters
default_args = {
    'owner': 'yates',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def run_extract_load():
    """Execute the ETL process: extract data from CSV and load to PostgreSQL."""
    extract.main()  # Convert CSV to Parquet
    load.main()     # Load Parquet to PostgreSQL


# Define the DAG and its tasks
with DAG(
    'pipeline',
    default_args=default_args,
    description='Sales Data ELT Pipeline',
    schedule='@daily',
    catchup=False,
) as dag:

    # Task 1: Create necessary directories for data storage
    create_directories = PythonOperator(
        task_id='create_directories',
        python_callable=create_data_directories,
    )

    # Task 2: Start PostgreSQL database using Docker
    DOCKER_DIR = Path(__file__).resolve().parent.parent
    create_database = BashOperator(
        task_id='create_database',
        bash_command=(
            "cd {{ params.docker_dir }} && "
            "docker-compose up"
        ),
        params={"dbt_dir": str(DOCKER_DIR)}
    )

    # Task 3: Run ETL process
    run_elt = PythonOperator(
        task_id='run_extract_load',
        python_callable=run_extract_load,
    )

    # Task 4: Transform data using dbt models
    DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"
    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --profiles-dir {{ params.dbt_dir }}/profiles --project-dir {{ params.dbt_dir }}"
            ),
        params={"dbt_dir": str(DBT_DIR)}
    )

    # Define task dependencies: directories -> database -> ETL -> dbt
    create_directories >> create_database >> run_elt >> run_dbt_model