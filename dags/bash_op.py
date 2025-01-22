from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator

import pendulum
from datetime import datetime, timedelta

default_args={
    'owner':'bash_check',
    'retries':1,
    'retry_dealy':timedelta(minutes=2)
}
# Dag Creation
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
    dummy_operator_task = DummyOperator(
    task_id = 'dummy_operator_task',
    dag = dag
)
    
    email_task=EmailOperator(
        task_id='task_email',
        to='joharamandeep99@gmail.com',
        subject='ETL Task',
        html_content='this is test'
    )
    
    task1>> dummy_operator_task >>email_task