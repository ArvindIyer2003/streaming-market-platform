from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "arvind",
    "start_date": datetime(2024,1,1),
}

with DAG(
    "market_data_pipeline",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False
) as dag:

    bronze = BashOperator(
        task_id="bronze_layer",
        bash_command="python ~/streaming_market_platform/spark_jobs/bronze/bronze_layer.py"
    )

    silver = BashOperator(
        task_id="silver_layer",
        bash_command="python ~/streaming_market_platform/spark_jobs/silver/silver_layer.py"
    )

    gold = BashOperator(
        task_id="gold_layer",
        bash_command="python ~/streaming_market_platform/spark_jobs/gold/gold_layer.py"
    )

    snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python ~/streaming_market_platform/snowflake/load_to_snowflake.py"
    )

    bronze >> silver >> gold >> snowflake