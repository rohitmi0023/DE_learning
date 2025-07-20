from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id = 'demo_dag',
    start_date = datetime(2025,07,19),
    schedule_interval = "@hourly",
    catchup = False
) as dag:
    taskA = DummyOperator(task_id='start', dag=dag)
    taskB = DummyOperator(task_id='end', dag=dag)
    taskA >> taskB

# 2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

with DAG(
    dag_id = 'executor',
    start_date = datetime(2025, 07, 19),
    schedule_interval = "@hourly",
    catchup = False
) as dag:
    def hello_function():
        print("Hello Everyone!")
        time.sleep(5)
    
    def last_function():
        print("DAG run is done")

    def sleeping_function():
        print("Sleeping function for 5 seconds")
        time.sleep(5)

    task1 = PythonOperator(task_id='hello_function', python_callable = hello_function)
    task2_1 = PythonOperator(task_id='Sleepy1', python_callable = sleeping_function)
    task2_2 = PythonOperator(task_id='Sleepy2', python_callable = sleeping_function)
    task3 = PythonOperator(task_id='Completed', python_callable = last_function) 
    task1 >> [task2_1, task2_2] >> task3