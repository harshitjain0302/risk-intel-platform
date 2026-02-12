from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os

RAW_DIR = "/opt/airflow/data/local/raw/unsw_nb15"
OUT_DIR = "/opt/airflow/data/local/curated/features"

# Minimal, interview-friendly feature subset (you can expand later)
FEATURE_COLS = [
    "dur", "spkts", "dpkts", "sbytes", "dbytes",
    "rate", "sload", "dload", "sttl", "dttl"
]

TARGET_COL = "label"          # binary 0/1
ATTACK_CAT_COL = "attack_cat" # optional multi-class

def read_csv(spark: SparkSession, path: str):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )

def main():
    spark = SparkSession.builder.appName("risk-intel-unsw-etl-local").getOrCreate()

    train_path = os.path.join(RAW_DIR, "UNSW_NB15_training-set.csv")
    test_path  = os.path.join(RAW_DIR, "UNSW_NB15_testing-set.csv")

    df_train = read_csv(spark, train_path)
    df_test  = read_csv(spark, test_path)

    # Union train+test to form one curated dataset (we’ll split later in ML step)
    df = df_train.unionByName(df_test, allowMissingColumns=True)

    # Keep only columns that actually exist (dataset versions sometimes differ)
    keep = [c for c in (FEATURE_COLS + [TARGET_COL, ATTACK_CAT_COL]) if c in df.columns]
    df = df.select(*keep)

    # Ensure numeric features are numeric + fill nulls
    for c in FEATURE_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    if TARGET_COL in df.columns:
        df = df.withColumn(TARGET_COL, F.col(TARGET_COL).cast("int"))

    df = df.fillna(0)

    # Add a stable row id for downstream joins/debug
    df = df.withColumn("row_id", F.monotonically_increasing_id())

    (
        df.write
        .mode("overwrite")
        .parquet(OUT_DIR)
    )

    print(f"[OK] Wrote curated Parquet to: {OUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()