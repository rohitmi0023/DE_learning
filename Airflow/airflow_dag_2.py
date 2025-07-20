from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'rohit',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'uniquee',
    default_args = default_args,
    description = 'descccc',
    start_date = datetime(2025, 07, 19),
    schedule_inteval = '@hourly'
) as dag:
    task1 = BashOperator(tak_id = 'first_task', bash_command = 'echo, hello!!') 

    task1