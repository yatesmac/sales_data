import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# sys.path lists directories that Python searches for modules to import
sys.path.append('../src')
import extract
import load
from create_data_dir import create_data_directories


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

    DOCKER_DIR = Path(__file__).resolve().parent.parent
    create_database = BashOperator(
        task_id='create_database',
        bash_command=(
            "cd {{ params.docker_dir }} && "
            "docker-compose up"
        ),
        params={"dbt_dir": str(DOCKER_DIR)}
    )

    run_elt = PythonOperator(
        task_id='run_extract_load',
        python_callable=run_extract_load,
    )

    DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"
    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --profiles-dir {{ params.dbt_dir }}/profiles --project-dir {{ params.dbt_dir }}"
            ),
        params={"dbt_dir": str(DBT_DIR)}
    )

    # Set task dependencies
    create_directories >> create_database >> run_elt >> run_dbt_model