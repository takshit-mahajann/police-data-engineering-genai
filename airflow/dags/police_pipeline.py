from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = "/mnt/d/AmitTrivediProj"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="cambridge_police_pipeline",

    start_date=datetime(2026, 8, 17),

    # Run at minute 0 of every hour
    schedule="0 * * * *",

    catchup=False,

    default_args=default_args,

    # Prevent overlapping pipeline runs
    max_active_runs=1,

    tags=[
        "spark",
        "police",
        "data-engineering",
    ],
) as dag:

    ingest_data = BashOperator(
        task_id="ingest_data",

        bash_command=(
            f"cd {PROJECT_ROOT}/ingestion && "
            "python ingestion.py"
        ),

        # Don't allow a stuck API call to run forever
        execution_timeout=timedelta(minutes=15),
    )


    run_spark_pipeline = BashOperator(
        task_id="run_spark_pipeline",

        bash_command=(
            f"cd {PROJECT_ROOT}/sparkModule && "
            "python transform.py"
        ),

        # Don't allow Spark to run forever
        execution_timeout=timedelta(minutes=30),
    )


    # Ingestion MUST finish successfully before Spark starts
    ingest_data >> run_spark_pipeline