from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

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
        'data/datalake/categories',
        'data/datalake/cities',
        'data/datalake/countries',
        'data/datalake/customers',
        'data/datalake/employees',
        'data/datalake/products',
        'data/datalake/sales'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def run_extract_load():
    """Run the extract and load process."""
    from extract_load import main
    main()

with DAG(
    'sales_data_elt',
    default_args=default_args,
    description='Sales Data ELT Pipeline',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    create_directories = PythonOperator(
        task_id='create_directories',
        python_callable=create_data_directories,
    )

    create_database = BashOperator(
        task_id='create_database',
        bash_command='docker exec postgres psql -U postgres -c "CREATE DATABASE sales_data;" || true',
    )

    create_schema = BashOperator(
        task_id='create_schema',
        bash_command='docker exec postgres psql -U postgres -d sales_data -f /docker-entrypoint-initdb.d/schema.sql',
    )

    run_elt = PythonOperator(
        task_id='run_extract_load',
        python_callable=run_extract_load,
    )

    # Set task dependencies
    create_directories >> create_database >> create_schema >> run_elt 