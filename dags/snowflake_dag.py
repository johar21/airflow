import datetime
import pendulum
from datetime import timedelta, datetime

from airflow.models import DAG

# https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/operators/snowflake.html
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


query_test = (
    "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CATALOG_RETURNS LIMIT 1;"
)

from airflow.models import Variable

snowflake_conn_id = Variable.get("snowflake_conn_id", default_var="snowflake_conn_id")


default_args = {"owner": "snow", "retries": 1, "retry_dealy": timedelta(minutes=2)}
# Dag Creation
with DAG(
    dag_id="Snowflake",
    description="Snowflake",
    default_args=default_args,
    start_date=datetime(2015, 1, 21, tzinfo=pendulum.timezone("America/Los_Angeles")),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    #
    snowflake = SnowflakeOperator(
        task_id="test_snowflake_connection",
        sql=query_test,
        snowflake_conn_id=snowflake_conn_id,
        dag=dag,
    )
