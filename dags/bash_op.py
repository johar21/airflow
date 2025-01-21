from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum
from datetime import datetime, timedelta

default_args={
    'owner':'bash_check',
    'retries':1,
    'retry_dealy':timedelta(minutes=2)
}

with DAG(
    dag_id='bash_check',
    description='Bash Operator dag',
    default_args=default_args,
    start_date=datetime(2015,1,21, tzinfo=pendulum.timezone("America/Los_Angeles")),
    schedule_interval='@daily',
    catchup=False 
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello world!!"
    )
    
    task1