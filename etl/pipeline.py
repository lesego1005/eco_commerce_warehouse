# etl/pipeline.py
import sys
import os
import traceback
import logging
import pandas as pd
from sqlalchemy import create_engine

# Dynamically add project root to sys.path FIRST
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now safe to import from etl package
from etl.extract import extract_all, extract_streaming_updates
from etl.transform import transform_all
from etl.load import load_all

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Phase 8: Database connection for quality logging
engine = create_engine('postgresql://postgres:Money12!@127.0.0.1:6432/eco_warehouse')

def log_quality_metrics(data_dict):
    """Phase 8: Logs data quality metrics to the database"""
    logger.info("Phase 8: Tracking Data Quality Metrics...")
    for table_name, df in data_dict.items():
        if df is None or df.empty:
            continue
            
        total_rows = len(df)
        null_counts = int(df.isnull().sum().sum())
        # Checking for duplicates based on all columns
        duplicate_counts = int(df.duplicated().sum())
        
        status = 'PASS' if null_counts == 0 else 'WARNING'
        
        quality_data = pd.DataFrame([{
            'table_name': table_name,
            'total_rows': total_rows,
            'null_counts': null_counts,
            'duplicate_counts': duplicate_counts,
            'status': status
        }])
        
        try:
            quality_data.to_sql('data_quality_log', engine, if_exists='append', index=False)
            logger.info(f"Quality Logged for {table_name}: {null_counts} nulls found.")
        except Exception as e:
            logger.error(f"Failed to write quality metrics: {e}")

def run_etl(staging_dir="staging"):
    logger.info("===== Starting ETL Pipeline =====")
    
    if not os.path.isdir(staging_dir):
        logger.warning(f"Staging directory '{staging_dir}' does not exist. Checking streaming only...")
    elif not os.listdir(staging_dir):
        logger.warning(f"Staging directory '{staging_dir}' is empty. Checking streaming only...")

    try:
        # Step 1: Extract batch data
        logger.info("Step 1: Extracting batch data from staging...")
        raw_data = extract_all(staging_dir)
        
        if not raw_data:
            logger.info("No batch files found — checking for real-time streaming updates...")
            streaming_df = extract_streaming_updates()
            
            if streaming_df.empty:
                logger.warning("No batch files and no streaming updates found. Exiting.")
                return
            
            raw_data = {'products': streaming_df}
            logger.info(f"Using {len(streaming_df)} streaming updates as product source")

        else:
            logger.info(f"Batch data loaded: {list(raw_data.keys())}")
            streaming_df = extract_streaming_updates()
            if not streaming_df.empty and 'products' in raw_data and not raw_data['products'].empty:
                logger.info("Applying real-time streaming updates to batch products...")
                products = raw_data['products']
                merged = products.merge(
                    streaming_df[['product_name', 'new_price']],
                    on='product_name',
                    how='left'
                )
                merged['price'] = merged['new_price'].combine_first(merged['price'])
                merged = merged.drop(columns=['new_price'], errors='ignore')
                raw_data['products'] = merged

        # Step 2: Transform (clean, rename, enrich, outliers)
        logger.info("Step 2: Transforming data...")
        transformed_data = transform_all(raw_data)
        
        # Phase 8: Track Metrics AFTER transformation
        log_quality_metrics(transformed_data)

        # Step 3: Load to PostgreSQL
        logger.info("Step 3: Loading to PostgreSQL warehouse...")
        load_all(transformed_data)

        logger.info("===== ETL Pipeline completed successfully =====")
        
    except Exception as e:
        logger.error("===== ETL Pipeline failed =====")
        # Phase 8: Error Tracking
        error_df = pd.DataFrame([{
            'table_name': 'PIPELINE_ERROR',
            'status': 'FAILED',
            'null_counts': 0,
            'total_rows': 0,
            'duplicate_counts': 0
        }])
        error_df.to_sql('data_quality_log', engine, if_exists='append', index=False)
        
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    run_etl()