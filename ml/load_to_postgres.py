import pandas as pd
from sqlalchemy import create_engine

PARQUET_PATH = "/opt/airflow/data/local/curated/features"
PG_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def main():
    df = pd.read_parquet(PARQUET_PATH)
    engine = create_engine(PG_URL)
    df.to_sql("curated_events", engine, if_exists="replace", index=False)

if __name__ == "__main__":
    main()
