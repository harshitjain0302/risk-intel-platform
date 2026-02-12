import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.linear_model import LogisticRegression
import mlflow

PG_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def main():
    # LOCAL MLflow backend (no server dependency)
    mlflow.set_tracking_uri("file:/opt/airflow/data/local/artifacts/mlruns")
    mlflow.set_experiment("risk-intel-hello")

    engine = create_engine(PG_URL)
    df = pd.read_sql("select * from curated_events", engine)

    for c in ["dur","spkts","dpkts","sbytes","dbytes"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    X = df[["dur","spkts","dpkts","sbytes","dbytes"]]
    y = df["label"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    with mlflow.start_run():
        model = LogisticRegression(max_iter=200)
        model.fit(X_train, y_train)

        preds = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, preds) if len(set(y_test)) > 1 else 0.0

        mlflow.log_param("model", "logistic_regression")
        mlflow.log_metric("roc_auc", float(auc))

if __name__ == "__main__":
    main()