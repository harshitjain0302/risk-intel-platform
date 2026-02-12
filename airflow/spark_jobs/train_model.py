"""
Train attack detection classifier with MLflow tracking
"""
import mlflow
import mlflow.sklearn
import psycopg2
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, roc_auc_score, confusion_matrix
)
from sklearn.preprocessing import StandardScaler
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'warehouse',
    'user': 'airflow',
    'password': 'airflow'
}

MLFLOW_TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "unsw-attack-detection"

# Features to use for modeling
FEATURE_COLS = [
    'dur', 'sbytes', 'dbytes', 'sttl', 'dttl',
    'sloss', 'dloss', 'sload', 'dload', 'spkts', 'dpkts'
]

TARGET_COL = 'label'


def load_data_from_postgres():
    """Load training data from Postgres"""
    logger.info("Loading data from Postgres...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = f"""
        SELECT {', '.join(FEATURE_COLS)}, {TARGET_COL}, dataset_split, attack_cat
        FROM curated_unsw_flows
        WHERE dataset_split = 'train'
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Loaded {len(df):,} training samples")
    logger.info(f"Attack distribution: {df[TARGET_COL].value_counts().to_dict()}")
    
    return df


def prepare_features(df):
    """Prepare features for modeling"""
    logger.info("Preparing features...")
    
    # Handle missing values
    df = df.fillna(0)
    
    # Handle infinite values
    df = df.replace([np.inf, -np.inf], 0)
    
    X = df[FEATURE_COLS].values
    y = df[TARGET_COL].values
    
    # Split train/val
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    
    logger.info(f"Train size: {len(X_train):,}, Val size: {len(X_val):,}")
    
    return X_train_scaled, X_val_scaled, y_train, y_val, scaler


def train_model(X_train, y_train, X_val, y_val):
    """Train logistic regression classifier"""
    logger.info("Training model...")
    
    # Simple baseline model
    model = LogisticRegression(
        max_iter=1000,
        random_state=42,
        class_weight='balanced'  # Handle class imbalance
    )
    
    model.fit(X_train, y_train)
    
    # Predictions
    y_train_pred = model.predict(X_train)
    y_val_pred = model.predict(X_val)
    y_val_proba = model.predict_proba(X_val)[:, 1]
    
    # Calculate metrics
    metrics = {
        'train_accuracy': accuracy_score(y_train, y_train_pred),
        'val_accuracy': accuracy_score(y_val, y_val_pred),
        'val_precision': precision_score(y_val, y_val_pred),
        'val_recall': recall_score(y_val, y_val_pred),
        'val_f1': f1_score(y_val, y_val_pred),
        'val_auc': roc_auc_score(y_val, y_val_proba)
    }
    
    logger.info("Model Performance:")
    for metric, value in metrics.items():
        logger.info(f"  {metric}: {value:.4f}")
    
    # Confusion matrix
    cm = confusion_matrix(y_val, y_val_pred)
    logger.info(f"Confusion Matrix:\n{cm}")
    
    return model, metrics


def main():
    """Main training pipeline with MLflow tracking"""
    
    # Set MLflow tracking
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    logger.info(f"MLflow tracking URI: {MLFLOW_TRACKING_URI}")
    logger.info(f"Experiment: {EXPERIMENT_NAME}")
    
    # Start MLflow run
    with mlflow.start_run(run_name="logistic_regression_baseline"):
        
        # Log parameters
        mlflow.log_param("model_type", "LogisticRegression")
        mlflow.log_param("features", FEATURE_COLS)
        mlflow.log_param("dataset_source", "curated_unsw_flows")
        mlflow.log_param("train_test_split", "dataset_split=train")
        
        # Step 1: Load data
        df = load_data_from_postgres()
        mlflow.log_param("total_samples", len(df))
        mlflow.log_param("attack_samples", int(df[TARGET_COL].sum()))
        mlflow.log_param("normal_samples", int((df[TARGET_COL] == 0).sum()))
        
        # Step 2: Prepare features
        X_train, X_val, y_train, y_val, scaler = prepare_features(df)
        mlflow.log_param("train_samples", len(X_train))
        mlflow.log_param("val_samples", len(X_val))
        
        # Step 3: Train model
        model, metrics = train_model(X_train, y_train, X_val, y_val)
        
        # Step 4: Log metrics to MLflow
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)
        
        # Note: Model artifact saving skipped due to permissions
        # Metrics and parameters are the key tracking for experiments
        
        logger.info("✅ Model training complete and logged to MLflow!")
        logger.info(f"Run ID: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    main()