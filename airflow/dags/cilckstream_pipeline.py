from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "admin",
    "depends_on_past": False
}


with DAG(
    dag_id="clickstream_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 25),
    schedule_interval="@daily",
    catchup=False
) as dag:

    generate_data = BashOperator(
        task_id="generate_clickstream",
        bash_command="""
        cd /opt/airflow &&
        python /opt/generator/generate_clickstream.py
        """
    )

    bronze = BashOperator(
        task_id="bronze_layer",
        bash_command="""
        docker exec spark-master \
        spark-submit /opt/spark_jobs/bronze_job.py
        """
    )

    silver = BashOperator(
        task_id="silver_layer",
        bash_command="""
        docker exec spark-master \
        spark-submit /opt/spark_jobs/silver_job.py
        """
    )

    gold = BashOperator(
        task_id="gold_layer",
        bash_command="""
        docker exec spark-master \
        spark-submit /opt/spark_jobs/gold_job.py
        """
    )

    generate_data >> bronze >> silver >> gold