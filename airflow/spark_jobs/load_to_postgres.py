"""
Load curated UNSW-NB15 data from S3 into Postgres warehouse
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Postgres connection settings
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'warehouse',
    'user': 'airflow',
    'password': 'airflow'
}

# S3 configuration
S3_BUCKET = 'risk-intel-platform-harshit'
S3_PREFIX = 'curated/unsw_nb15/'


def create_tables(conn):
    """Create analytics tables if they don't exist"""
    with conn.cursor() as cur:
        # Main curated flows table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS curated_unsw_flows (
                id SERIAL PRIMARY KEY,
                dataset_split VARCHAR(20),
                traffic_volume VARCHAR(20),
                srcip VARCHAR(45),
                sport INTEGER,
                dstip VARCHAR(45),
                dsport INTEGER,
                proto VARCHAR(20),
                state VARCHAR(50),
                dur FLOAT,
                sbytes BIGINT,
                dbytes BIGINT,
                sttl INTEGER,
                dttl INTEGER,
                sloss INTEGER,
                dloss INTEGER,
                service VARCHAR(50),
                sload FLOAT,
                dload FLOAT,
                spkts INTEGER,
                dpkts INTEGER,
                attack_cat VARCHAR(100),
                label INTEGER,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Summary table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS attack_summary (
                id SERIAL PRIMARY KEY,
                dataset_split VARCHAR(20),
                attack_cat VARCHAR(100),
                label INTEGER,
                flow_count INTEGER,
                avg_duration FLOAT,
                total_bytes BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("✓ Tables created/verified")


def load_parquet_from_s3_to_postgres(conn):
    """Load all curated Parquet files from S3 into Postgres"""
    flows_loaded = 0
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    logger.info(f"Listing files in s3://{S3_BUCKET}/{S3_PREFIX}")
    
    # List all parquet files in S3
    paginator = s3.get_paginator('list_objects_v2')
    parquet_files = []
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    parquet_files.append(obj['Key'])
    
    logger.info(f"Found {len(parquet_files)} parquet files to load")
    
    for s3_key in parquet_files:
        logger.info(f"Loading {s3_key.split('/')[-1]}...")
        
        # Read parquet from S3
        s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        df = pd.read_parquet(s3_path)
        
        # Extract partition values from path
        if 'dataset_split=train' in s3_key:
            df['dataset_split'] = 'train'
        elif 'dataset_split=test' in s3_key:
            df['dataset_split'] = 'test'
        
        if 'traffic_volume=high' in s3_key:
            df['traffic_volume'] = 'high'
        elif 'traffic_volume=medium' in s3_key:
            df['traffic_volume'] = 'medium'
        elif 'traffic_volume=low' in s3_key:
            df['traffic_volume'] = 'low'
        
        # Select columns to load (adjust based on your actual schema)
        columns = [
            'dataset_split', 'traffic_volume', 'srcip', 'sport', 'dstip', 
            'dsport', 'proto', 'state', 'dur', 'sbytes', 'dbytes',
            'sttl', 'dttl', 'sloss', 'dloss', 'service', 
            'sload', 'dload', 'spkts', 'dpkts', 'attack_cat', 'label'
        ]
        
        # Filter to only columns that exist
        available_cols = [col for col in columns if col in df.columns]
        df_insert = df[available_cols]
        
        # Insert in batches
        with conn.cursor() as cur:
            cols_str = ', '.join(available_cols)
            values = [tuple(row) for row in df_insert.values]
            
            execute_values(
                cur,
                f"INSERT INTO curated_unsw_flows ({cols_str}) VALUES %s",
                values,
                page_size=1000
            )
        
        flows_loaded += len(df)
        logger.info(f"  → Loaded {len(df)} rows")
    
    conn.commit()
    logger.info(f"✓ Total flows loaded: {flows_loaded:,}")
    return flows_loaded


def build_summary_table(conn):
    """Build attack summary aggregations"""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE attack_summary")
        
        cur.execute("""
            INSERT INTO attack_summary (
                dataset_split, attack_cat, label, 
                flow_count, avg_duration, total_bytes
            )
            SELECT 
                dataset_split,
                attack_cat,
                label,
                COUNT(*) as flow_count,
                AVG(dur) as avg_duration,
                SUM(sbytes + dbytes) as total_bytes
            FROM curated_unsw_flows
            GROUP BY dataset_split, attack_cat, label
            ORDER BY flow_count DESC
        """)
        
        conn.commit()
        logger.info("✓ Summary table built")


def main():
    """Main ETL process"""
    logger.info("Starting Postgres load from S3...")
    
    # Connect to Postgres
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Step 1: Create tables
        create_tables(conn)
        
        # Step 2: Clear existing data (idempotent loads)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE curated_unsw_flows CASCADE")
            conn.commit()
        logger.info("✓ Cleared existing data")
        
        # Step 3: Load parquet files from S3
        flows_loaded = load_parquet_from_s3_to_postgres(conn)
        
        # Step 4: Build summary
        build_summary_table(conn)
        
        logger.info(f"✅ Postgres load complete! {flows_loaded:,} flows loaded from S3")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()