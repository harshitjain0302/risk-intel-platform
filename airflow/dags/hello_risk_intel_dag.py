from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_risk_intel",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["risk-intel", "hello"],
) as dag:

    spark_etl = BashOperator(
        task_id="spark_etl_to_parquet",
        bash_command="python /opt/airflow/spark_jobs/etl_local.py",
    )

    load_pg = BashOperator(
        task_id="load_parquet_to_postgres",
        bash_command="python /opt/airflow/ml/load_to_postgres.py",
    )

    train = BashOperator(
        task_id="train_and_log_mlflow",
        bash_command="python /opt/airflow/ml/train_baseline.py",
    )

    spark_etl >> load_pg >> train
