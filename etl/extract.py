# etl/extract.py
import os
import pandas as pd
import json
import logging
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_file(file_path: str):
    """Extract single file based on extension."""
    ext = os.path.splitext(file_path)[1].lower()
    
    try:
        if ext == '.csv':
            df = pd.read_csv(file_path)
            logger.info(f"Extracted CSV: {file_path} ({len(df)} rows)")
            return df
        
        elif ext == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            logger.info(f"Extracted JSON: {file_path} ({len(df)} rows)")
            return df
        
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
            logger.info(f"Extracted Excel: {file_path} ({len(df)} rows)")
            return df
        
        else:
            logger.warning(f"Unsupported file type: {file_path}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to extract {file_path}: {str(e)}")
        return None


def extract_streaming_updates(streaming_dir: str = "staging/streaming_updates"):
    """Load all recent real-time product updates from Kafka consumer files."""
    if not os.path.isdir(streaming_dir):
        logger.info(f"Streaming directory not found: {streaming_dir} - skipping")
        return pd.DataFrame()

    updates = []
    json_files = glob.glob(os.path.join(streaming_dir, "*.json"))
    
    if not json_files:
        logger.info("No streaming update files found")
        return pd.DataFrame()

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    updates.append(data)
                else:
                    logger.warning(f"Invalid JSON structure in {file_path}")
        except Exception as e:
            logger.error(f"Error reading streaming file {file_path}: {e}")

    if updates:
        df_updates = pd.DataFrame(updates)
        logger.info(f"Loaded {len(df_updates)} real-time product updates from streaming")
        return df_updates
    
    return pd.DataFrame()


def extract_all(staging_dir: str = "staging", apply_streaming: bool = True) -> dict:
    """Extract all relevant files from staging directory + apply real-time streaming updates."""
    if not os.path.isdir(staging_dir):
        raise FileNotFoundError(f"Staging directory not found: {staging_dir}")

    data = {
        'sales': None,
        'products': None,
        'customers': None
    }

    # Extract batch files
    for filename in os.listdir(staging_dir):
        full_path = os.path.join(staging_dir, filename)
        if not os.path.isfile(full_path):
            continue

        df = extract_file(full_path)
        if df is None:
            continue

        # Assign based on filename pattern
        if 'sales' in filename.lower():
            data['sales'] = df
        elif 'products' in filename.lower():
            data['products'] = df
        elif 'customers' in filename.lower():
            data['customers'] = df

    # Basic validation
    missing = [k for k, v in data.items() if v is None]
    if missing:
        logger.warning(f"Missing batch data sources: {missing}")

    # Apply real-time streaming updates (if enabled and products exist)
    if apply_streaming:
        streaming_df = extract_streaming_updates()
        if not streaming_df.empty:
            if 'products' in data and data['products'] is not None and not data['products'].empty:
                logger.info("Merging real-time streaming updates into products dimension")
                products = data['products']

                # Merge on product_name - update price if streamed value exists
                merged = products.merge(
                    streaming_df[['product_name', 'new_price']],
                    on='product_name',
                    how='left'
                )
                # Use streamed price if available, else keep original
                merged['price'] = merged['new_price'].combine_first(merged['price'])
                merged = merged.drop(columns=['new_price'], errors='ignore')

                data['products'] = merged
                logger.info(f"Applied {len(streaming_df)} streaming updates to products")
            else:
                logger.warning("No batch products found - cannot apply streaming updates")

    return {k: v for k, v in data.items() if v is not None}