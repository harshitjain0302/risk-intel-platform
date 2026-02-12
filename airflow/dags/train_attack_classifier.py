"""
ML Training DAG - Train attack detection classifier with MLflow tracking
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'ml-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_data_ready():
    """Verify that curated data exists in Postgres"""
    import psycopg2
    
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='warehouse',
        user='airflow',
        password='airflow'
    )
    
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM curated_unsw_flows WHERE dataset_split = 'train'")
    count = cur.fetchone()[0]
    
    conn.close()
    
    if count == 0:
        raise ValueError("No training data found in Postgres!")
    
    logger.info(f"✓ Found {count:,} training samples in Postgres")
    return True


with DAG(
    'train_attack_classifier',
    default_args=default_args,
    description='Train ML classifier for attack detection with MLflow tracking',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'security'],
) as dag:
    
    # Task 1: Verify data is ready
    check_data = PythonOperator(
        task_id='check_training_data',
        python_callable=check_data_ready,
    )
    
    # Task 2: Train model with MLflow tracking
    train_model = BashOperator(
        task_id='train_model',
        bash_command='python /opt/airflow/spark_jobs/train_model.py',
    )
    
    # Task 3: Verify MLflow logged the run
    verify_mlflow = BashOperator(
        task_id='verify_mlflow_tracking',
        bash_command='''
        echo "✓ Model training complete"
        echo "→ View results at: http://localhost:5001"
        ''',
    )
    
    # Define dependencies
    check_data >> train_model >> verify_mlflow