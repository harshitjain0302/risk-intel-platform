from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def check_data_exists():
    """Verify input data files exist"""
    import os
    
    train_file = "/opt/airflow/data/local/raw/unsw_nb15/UNSW_NB15_training-set.csv"
    test_file = "/opt/airflow/data/local/raw/unsw_nb15/UNSW_NB15_testing-set.csv"
    
    if not os.path.exists(train_file):
        raise FileNotFoundError(f"Training file not found: {train_file}")
    
    if not os.path.exists(test_file):
        raise FileNotFoundError(f"Test file not found: {test_file}")
    
    logger.info("✓ All input files found")
    return True

with DAG(
    'unsw_etl_pipeline',
    default_args=default_args,
    description='UNSW-NB15 intrusion detection ETL pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'security', 'unsw'],
) as dag:
    
    # Task 1: Check data exists
    check_data = PythonOperator(
        task_id='check_input_data',
        python_callable=check_data_exists,
    )
    
    # Task 2: Run PySpark ETL
    run_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='python /opt/airflow/spark_jobs/etl_unsw_to_curated.py',
    )
    
    # Task 3: Verify output
    verify_output = BashOperator(
        task_id='verify_curated_data',
        bash_command='''
        if [ -d "/opt/airflow/data/local/curated/unsw_nb15" ]; then
            echo "✓ Curated data directory exists"
            ls -lh /opt/airflow/data/local/curated/unsw_nb15
            exit 0
        else
            echo "✗ Curated data not found"
            exit 1
        fi
        ''',
    )

    # Task 4: Load to Postgres
    load_postgres = BashOperator(
        task_id='load_to_postgres',
        bash_command='python /opt/airflow/spark_jobs/load_to_postgres.py',
    )
    
    # Define dependencies
    check_data >> run_etl >> verify_output >> load_postgres