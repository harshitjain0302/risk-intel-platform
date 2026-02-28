from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, 
    log1p, regexp_replace, trim, upper,
    hour, minute, dayofweek
)
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("UNSW-ETL") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.0") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_raw_data(spark, train_path, test_path):
    """Load training and testing CSV files"""
    logger.info("Loading raw data...")
    
    # Read training data
    df_train = spark.read.csv(
        train_path,
        header=True,
        inferSchema=True
    ).withColumn("dataset_split", lit("train"))
    
    # Read testing data
    df_test = spark.read.csv(
        test_path,
        header=True,
        inferSchema=True
    ).withColumn("dataset_split", lit("test"))
    
    # Combine
    df = df_train.union(df_test)
    
    logger.info(f"Loaded {df.count()} total records")
    logger.info(f"  - Training: {df_train.count()}")
    logger.info(f"  - Testing: {df_test.count()}")
    
    return df

def clean_data(df):
    """Clean and standardize data"""
    logger.info("Cleaning data...")
    
    # Standardize column names (remove spaces, lowercase)
    for col_name in df.columns:
        new_name = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_name)
    
    # Remove duplicates
    initial_count = df.count()
    df = df.dropDuplicates()
    logger.info(f"Removed {initial_count - df.count()} duplicates")
    
    # Handle missing values
    # For numeric columns, fill with 0
    numeric_cols = [field.name for field in df.schema.fields 
                   if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType))]
    
    for col_name in numeric_cols:
        df = df.fillna({col_name: 0})
    
    # For string columns, fill with 'unknown'
    string_cols = [field.name for field in df.schema.fields 
                  if isinstance(field.dataType, StringType)]
    
    for col_name in string_cols:
        df = df.fillna({col_name: 'unknown'})
    
    # Add metadata
    df = df.withColumn("load_timestamp", current_timestamp())
    
    logger.info(f"Cleaned data: {df.count()} records")
    return df

def engineer_features(df):
    """Create features for intrusion detection"""
    logger.info("Engineering features...")
    
    # 1. Categorize duration
    df = df.withColumn(
        "duration_category",
        when(col("dur") < 1, "very_short")
        .when((col("dur") >= 1) & (col("dur") < 10), "short")
        .when((col("dur") >= 10) & (col("dur") < 100), "medium")
        .when((col("dur") >= 100) & (col("dur") < 1000), "long")
        .otherwise("very_long")
    )
    
    # 2. Bytes transferred features
    df = df.withColumn("total_bytes", col("sbytes") + col("dbytes"))
    df = df.withColumn("byte_ratio", 
                       when(col("dbytes") > 0, col("sbytes") / col("dbytes"))
                       .otherwise(0))
    
    # 3. Log transformations (reduce skew)
    df = df.withColumn("log_sbytes", log1p(col("sbytes")))
    df = df.withColumn("log_dbytes", log1p(col("dbytes")))
    df = df.withColumn("log_dur", log1p(col("dur")))
    
    # 4. Protocol features
    df = df.withColumn("is_tcp", when(col("proto") == "tcp", 1).otherwise(0))
    df = df.withColumn("is_udp", when(col("proto") == "udp", 1).otherwise(0))
    
    # 5. State features
    df = df.withColumn("is_established", 
                       when(col("state").isin(["FIN", "CON"]), 1).otherwise(0))
    
    # 6. Attack type encoding (if exists)
    if "attack_cat" in df.columns:
        df = df.withColumn(
            "attack_type",
            when(col("attack_cat") == " ", "normal")
            .otherwise(trim(col("attack_cat")))
        )
        
        df = df.withColumn(
            "is_attack",
            when(col("label") == 1, 1).otherwise(0)
        )
    
    # 7. Traffic volume category
    df = df.withColumn(
        "traffic_volume",
        when(col("total_bytes") < 1000, "low")
        .when((col("total_bytes") >= 1000) & (col("total_bytes") < 100000), "medium")
        .otherwise("high")
    )
    
    logger.info("Feature engineering complete")
    return df

def data_quality_checks(df):
    """Run data quality validations"""
    logger.info("="*60)
    logger.info("DATA QUALITY CHECKS")
    logger.info("="*60)
    
    checks_passed = []
    checks_failed = []
    
    # Check 1: No null labels
    null_labels = df.filter(col("label").isNull()).count()
    if null_labels == 0:
        checks_passed.append("✓ No null labels")
    else:
        checks_failed.append(f"✗ Found {null_labels} null labels")
    
    # Check 2: Label values are valid (0 or 1)
    invalid_labels = df.filter(~col("label").isin([0, 1])).count()
    if invalid_labels == 0:
        checks_passed.append("✓ All labels are valid (0 or 1)")
    else:
        checks_failed.append(f"✗ Found {invalid_labels} invalid labels")
    
    # Check 3: Duration is non-negative
    negative_dur = df.filter(col("dur") < 0).count()
    if negative_dur == 0:
        checks_passed.append("✓ All durations are non-negative")
    else:
        checks_failed.append(f"✗ Found {negative_dur} negative durations")
    
    # Check 4: Bytes are non-negative
    negative_bytes = df.filter((col("sbytes") < 0) | (col("dbytes") < 0)).count()
    if negative_bytes == 0:
        checks_passed.append("✓ All byte counts are non-negative")
    else:
        checks_failed.append(f"✗ Found {negative_bytes} negative byte counts")
    
    # Check 5: Dataset split is valid
    valid_splits = df.filter(col("dataset_split").isin(["train", "test"])).count()
    if valid_splits == df.count():
        checks_passed.append("✓ All dataset splits are valid")
    else:
        checks_failed.append(f"✗ Found invalid dataset splits")
    
    # Print results
    for check in checks_passed:
        logger.info(check)
    
    for check in checks_failed:
        logger.error(check)
    
    logger.info(f"\nPassed: {len(checks_passed)}/{len(checks_passed) + len(checks_failed)} checks")
    logger.info("="*60)
    
    if checks_failed:
        raise ValueError(f"Data quality checks failed: {len(checks_failed)} issues found")
    
    return True

def write_curated_data(df, output_path):
    """Write partitioned Parquet files"""
    logger.info(f"Writing curated data to {output_path}")
    
    # Select final columns
    final_cols = [
        # Identifiers
        "dataset_split", "load_timestamp",
        
        # Original features (key ones)
        "srcip", "sport", "dstip", "dsport", "proto", "state",
        "dur", "sbytes", "dbytes", "sttl", "dttl",
        "sloss", "dloss", "service", "sload", "dload",
        
        # Engineered features
        "duration_category", "total_bytes", "byte_ratio",
        "log_sbytes", "log_dbytes", "log_dur",
        "is_tcp", "is_udp", "is_established",
        "traffic_volume",
        
        # Labels
        "label", "attack_cat", "attack_type", "is_attack"
    ]
    
    # Only keep columns that exist
    existing_cols = [c for c in final_cols if c in df.columns]
    df_final = df.select(existing_cols)
    
    # Write partitioned by dataset_split and traffic_volume
    df_final.write \
        .mode("overwrite") \
        .partitionBy("dataset_split", "traffic_volume") \
        .parquet(output_path)
    
    logger.info("✓ Data written successfully")
    
    # Print summary stats
    logger.info("\nSummary Statistics:")
    logger.info(f"Total records: {df_final.count()}")
    logger.info(f"Total attacks: {df_final.filter(col('is_attack') == 1).count()}")
    logger.info(f"Attack rate: {df_final.filter(col('is_attack') == 1).count() / df_final.count() * 100:.2f}%")
    
    # Show attack distribution
    logger.info("\nAttack Distribution:")
    df_final.groupBy("attack_type").count().orderBy(col("count").desc()).show()

def main():
    """Main ETL pipeline"""
    spark = create_spark_session()
    
    try:
        # Define paths
        train_path = "s3a://risk-intel-platform-harshit/raw/unsw_nb15/UNSW_NB15_training-set.csv"
        test_path = "s3a://risk-intel-platform-harshit/raw/unsw_nb15/UNSW_NB15_testing-set.csv"
        output_path = "s3a://risk-intel-platform-harshit/curated/unsw_nb15/"
        
        # ETL Pipeline
        logger.info("Starting UNSW-NB15 ETL Pipeline")
        logger.info("="*60)
        
        # Extract
        df_raw = load_raw_data(spark, train_path, test_path)
        
        # Transform
        df_clean = clean_data(df_raw)
        df_featured = engineer_features(df_clean)
        
        # Validate
        data_quality_checks(df_featured)
        
        # Load
        write_curated_data(df_featured, output_path)
        
        logger.info("="*60)
        logger.info("✓ ETL Pipeline completed successfully!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()