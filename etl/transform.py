# etl/transform.py
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def rename_to_schema_columns(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """Rename source columns to match PostgreSQL schema exactly."""
    df = df.copy()
    
    if table_type == 'products':
        rename_map = {
            'name': 'product_name',
            'carbon_rating': 'carbon_footprint_rating'
        }
        df = df.rename(columns=rename_map)
        
        # Ensure all expected columns exist (fill with None if missing)
        expected = ['product_name', 'category', 'price', 'carbon_footprint_rating']
        for col in expected:
            if col not in df.columns:
                df[col] = None
                logger.warning(f"Added missing column '{col}' with None values for products")
    
    elif table_type == 'customers':
        rename_map = {'name': 'customer_name'}  # Your generate_data.py uses 'name'
        df = df.rename(columns=rename_map)
        
        # Clean join_date: convert invalid to None (prevents NaT in SCD)
        if 'join_date' in df.columns:
            df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        
        expected = ['customer_name', 'email', 'loyalty_level', 'join_date']
        for col in expected:
            if col not in df.columns:
                df[col] = None
                logger.warning(f"Added missing column '{col}' with None values for customers")
    
    elif table_type == 'sales':
        # Sales typically doesn't need renaming at this stage (handled in enrich)
        pass
    
    return df


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning for sales data."""
    # Drop rows with critical missing values
    df = df.dropna(subset=['sale_id', 'product_name', 'quantity'])

    # Deduplicate
    df = df.drop_duplicates(subset=['sale_id'])

    # Fix data types
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce', downcast='integer')
    df['price'] = pd.to_numeric(df['price'].replace(r'[^\d.]', '', regex=True), errors='coerce')

    # Remove invalid rows
    df = df[(df['quantity'] > 0) & (df['price'] > 0)].dropna(subset=['quantity', 'price'])

    logger.info(f"After cleaning sales: {len(df)} rows remaining")
    return df


def enrich_sales(df_sales: pd.DataFrame, df_products: pd.DataFrame) -> pd.DataFrame:
    """Calculate revenue and carbon savings using renamed product_name."""
    if df_products is None or 'product_name' not in df_products.columns:
        logger.warning("No valid products DataFrame for enrichment - skipping carbon savings")
        df_sales['revenue'] = df_sales['quantity'] * df_sales['price']
        df_sales['carbon_savings'] = 0  # fallback
        return df_sales

    # Create lookup dictionary using product_name (after renaming)
    products_map = df_products.set_index('product_name')[['carbon_footprint_rating']].to_dict('index')
    
    def get_carbon_rating(product_name):
        if pd.isna(product_name):
            return 5
        norm_name = str(product_name).strip().lower()
        for p_name, info in products_map.items():
            if norm_name in str(p_name).strip().lower():
                return info['carbon_footprint_rating']
        return 5  # default if no match

    df_sales['carbon_footprint_rating'] = df_sales['product_name'].apply(get_carbon_rating)
    df_sales['revenue'] = df_sales['quantity'] * df_sales['price']
    df_sales['carbon_savings'] = df_sales['quantity'] * (10 - df_sales['carbon_footprint_rating'])

    logger.info("Enriched sales with revenue & carbon savings")
    return df_sales


def detect_outliers(df: pd.DataFrame, contamination=0.02) -> pd.DataFrame:
    """Remove outliers using Isolation Forest."""
    if len(df) < 10:
        logger.warning("Too few rows for outlier detection - skipping")
        return df

    features = ['quantity', 'revenue']
    model = IsolationForest(contamination=contamination, random_state=42)
    preds = model.fit_predict(df[features].fillna(0))

    clean_df = df[preds != -1].copy()
    logger.info(f"Removed {len(df) - len(clean_df)} outliers ({contamination*100:.1f}% target)")
    return clean_df


def transform_all(data: dict) -> dict:
    """Full transformation pipeline: rename → clean → enrich → outliers."""
    transformed = data.copy()

    # Rename columns first (critical for schema match)
    if 'products' in transformed:
        transformed['products'] = rename_to_schema_columns(transformed['products'], 'products')
    
    if 'customers' in transformed:
        transformed['customers'] = rename_to_schema_columns(transformed['customers'], 'customers')
    
    # Sales transformation
    if 'sales' in transformed:
        df_sales = clean_sales(transformed['sales'])
        df_products = transformed.get('products')
        df_sales = enrich_sales(df_sales, df_products)
        df_sales = detect_outliers(df_sales)
        transformed['sales'] = df_sales

    logger.info(f"Transformation complete. Sales rows: {len(transformed.get('sales', pd.DataFrame()))}")
    return transformed


def prepare_scd_df(df: pd.DataFrame, business_key: str, tracked_columns: list) -> pd.DataFrame:
    """Prepare dataframe for SCD Type 2 detection (optional hash for change detection)."""
    df = df.copy()
    df['business_key'] = df[business_key].astype(str).str.strip().str.lower()
    # Optional: hash for quick change detection (not used in current SCD but good to have)
    df['change_hash'] = df[tracked_columns].apply(lambda row: hash(tuple(row)), axis=1)
    return df


def map_fact_fks(df_sales: pd.DataFrame, conn) -> pd.DataFrame:
    """Map string keys to surrogate IDs from dimensions."""
    cursor = conn.cursor()
    
    # Get dim_date lookup (date → date_id)
    cursor.execute("SELECT date, date_id FROM dim_date")
    date_map = dict(cursor.fetchall())
    
    # Get dim_product lookup (product_name → product_id, current only)
    cursor.execute("SELECT product_name, product_id FROM dim_product WHERE is_current = TRUE")
    product_map = dict(cursor.fetchall())
    
    # Get dim_customer lookup (email → customer_id, current only)
    cursor.execute("SELECT email, customer_id FROM dim_customer WHERE is_current = TRUE")
    customer_map = dict(cursor.fetchall())
    
    # Get dim_location lookup (city → location_id)
    cursor.execute("SELECT city, location_id FROM dim_location")
    location_map = dict(cursor.fetchall())
    
    cursor.close()
    
    # Apply mappings
    df_sales['date_id'] = df_sales['date'].map(date_map)
    df_sales['product_id'] = df_sales['product_name'].map(product_map)
    df_sales['customer_id'] = df_sales['customer_email'].map(customer_map)
    df_sales['location_id'] = df_sales['city'].map(location_map)
    
    # Drop original string columns (not needed in fact table)
    drop_cols = ['date', 'product_name', 'customer_email', 'city']
    df_sales = df_sales.drop(columns=[c for c in drop_cols if c in df_sales.columns])
    
    # Final cleanup: drop rows with missing FKs (or log them)
    missing_fks = df_sales[['date_id', 'product_id', 'customer_id', 'location_id']].isna().any(axis=1)
    if missing_fks.any():
        logger.warning(f"{missing_fks.sum()} sales rows have missing FKs - dropping them")
        df_sales = df_sales[~missing_fks]
    
    # Rename quantity to match schema
    if 'quantity' in df_sales.columns:
        df_sales = df_sales.rename(columns={'quantity': 'quantity_sold'})
    
    logger.info(f"Fact table ready: {len(df_sales)} rows with FKs mapped")
    return df_sales