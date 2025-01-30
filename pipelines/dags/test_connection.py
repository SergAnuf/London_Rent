from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.test_connection import load,create_table
import pendulum

with DAG(
    dag_id='test_connection',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC")) as dag:

    create_t = PythonOperator(
        task_id='create',
        python_callable=create_table
    )
   
    load_step = PythonOperator(
        task_id='load',
        python_callable=load
    )

    # Set task dependencies
    create_t >> load_step
