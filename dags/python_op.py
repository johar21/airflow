from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Define a simple Python function
def print_hello():
    return "Hello, Airflow!"


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="python_operator_example",
    default_args=default_args,
    start_date=datetime(2025, 5, 8),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Define the PythonOperator task
    python_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,  # Function to execute
    )

    # Define task dependencies (if any)
    python_task
