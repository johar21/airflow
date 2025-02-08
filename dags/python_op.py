from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Define Python functions
def print_hello():
    print("Hello, Airflow!")


def print_bye():
    print("Bye!!")


# Default arguments for the DAG
default_args = {
    "owner": "test",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="python_operator_example",
    default_args=default_args,
    start_date=datetime(2024, 2, 1),  # Set a past date
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Define PythonOperator tasks
    python_task = PythonOperator(
        task_id="hello",
        python_callable=print_hello,  # Function to execute
    )

    python_bye = PythonOperator(
        task_id="bye",
        python_callable=print_bye,  # Function to execute
    )

    # Define task dependencies
    python_task >> python_bye  # hello → bye
