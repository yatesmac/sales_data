import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import extract
import load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_data_directories():
    """Create necessary data directories if they don't exist."""
    directories = [
        'data/raw',
        'data/datalake',
        'data/postgres',
        'data/postgres/vol-pgadmin_data',
        'data/postgres/vol-pgdata',
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def run_extract_load():
    """Run the extract and load process."""
    extract.main()
    load.main()

with DAG(
    'pipeline',
    default_args=default_args,
    description='Sales Data ELT Pipeline',
    schedule='@daily',
    catchup=False,
) as dag:

    create_directories = PythonOperator(
        task_id='create_directories',
        python_callable=create_data_directories,
    )

    create_database = BashOperator(
        task_id='create_database',
        bash_command='docker-compose up || true',
    )

    run_elt = PythonOperator(
        task_id='run_extract_load',
        python_callable=run_extract_load,
    )

    run_dbt_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='dbt run --models 04\ transform/'
    )

    # Set task dependencies
    create_directories >> create_database >> run_elt >> run_dbt_model