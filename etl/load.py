# etl/load.py
import sys
import os
import re
from fuzzywuzzy import fuzz, process

# Dynamically add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_conn():
    """Connection to local PostgreSQL via PgBouncer."""
    import os
    try:
        # UPDATED: Directing connection to PgBouncer port 6432 on localhost
        conn = psycopg2.connect(
            dbname=os.getenv("ECO_DB_NAME", "eco_warehouse"),
            user=os.getenv("ECO_DB_USER", "postgres"),
            password=os.getenv("ECO_DB_PASSWORD", ""),
            host=os.getenv("ECO_DB_HOST", "127.0.0.1"),  # Localhost instead of Docker host
            port=int(os.getenv("ECO_DB_PORT", 6432))     # PgBouncer port
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise

# ... [rest of the file remains exactly the same to ensure no logic breaks] ...

def upsert_df(df: pd.DataFrame, table_name: str, pk_columns: list, conn):
    """Bulk upsert using ON CONFLICT if constraint exists, else plain insert."""
    if df.empty:
        logger.info(f"No rows to upsert into {table_name}")
        return

    cursor = conn.cursor()
    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False)]

    conflict_target = ', '.join(pk_columns)
    update_set = ', '.join(f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_columns)

    query = f"""
    INSERT INTO {table_name} ({', '.join(cols)})
    VALUES %s
    ON CONFLICT ({conflict_target}) DO UPDATE SET
        {update_set}
    """

    try:
        execute_values(cursor, query, values)
        conn.commit()
        logger.info(f"Upserted {len(values)} rows into {table_name}")
    except psycopg2.errors.InvalidColumnReference:
        conn.rollback()
        logger.warning(f"No unique constraint on {pk_columns} for {table_name} - falling back to INSERT")
        insert_query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        execute_values(cursor, insert_query, values)
        conn.commit()
        logger.info(f"Inserted {len(values)} rows into {table_name} (no upsert)")
    except Exception as e:
        conn.rollback()
        logger.error(f"Upsert/Insert failed for {table_name}: {e}")
        raise

def handle_scd_type2(df_new: pd.DataFrame, table_name: str, business_key: str, tracked_cols: list, conn):
    """Proper SCD Type 2: expire old versions, insert new/changed."""
    if df_new.empty:
        logger.info(f"No new data for SCD on {table_name}")
        return

    cursor = conn.cursor()
    logger.info(f"SCD Type 2 for {table_name} - {len(df_new)} incoming rows")

    if business_key not in df_new.columns:
        logger.error(f"Missing business key '{business_key}' in data for {table_name}")
        raise KeyError(f"Missing '{business_key}'")

    df_new = df_new.copy()

    invalid_bk_mask = (
        df_new[business_key].isna() |
        df_new[business_key].astype(str).str.strip().eq('') |
        df_new[business_key].astype(str).str.strip().str.lower().eq('nan') |
        df_new[business_key].astype(str).str.strip().str.lower().eq('nat')
    )
    if invalid_bk_mask.any():
        logger.warning(f"Dropping {invalid_bk_mask.sum()} rows with invalid/missing/NaN/NaT {business_key}")
        df_new = df_new[~invalid_bk_mask].copy()

    if df_new.empty:
        logger.info(f"No valid rows left after cleaning {business_key}. Skipping SCD for {table_name}.")
        return

    df_new = df_new.drop_duplicates(subset=[business_key], keep='first')
    df_new['norm_key'] = df_new[business_key].astype(str).str.strip().str.lower()

    cursor.execute(f"SELECT * FROM {table_name} WHERE is_current = TRUE")
    existing_rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    existing_df = pd.DataFrame(existing_rows, columns=columns)

    existing_df['norm_key'] = existing_df[business_key].astype(str).str.strip().str.lower()

    if existing_df.empty:
        logger.info(f"First load for {table_name} - inserting all as current")
        df_new['effective_start'] = datetime.now()
        df_new['effective_end'] = 'infinity'
        df_new['is_current'] = True
        upsert_df(df_new.drop(columns=['norm_key'], errors='ignore'), table_name, [business_key], conn)
        return

    merged = df_new.merge(existing_df, left_on='norm_key', right_on='norm_key', suffixes=('_new', '_old'), how='outer')

    new_mask = merged['norm_key'].isin(df_new['norm_key']) & merged[business_key + '_old'].isna()
    new_records = merged[new_mask].copy()
    new_cols = [c.replace('_new', '') for c in merged.columns if '_new' in c]
    new_records = new_records[[c + '_new' for c in new_cols]].rename(columns=lambda x: x.replace('_new', ''))
    new_records['effective_start'] = datetime.now()
    new_records['effective_end'] = 'infinity'
    new_records['is_current'] = True

    timestamp_cols = ['effective_start', 'effective_end', 'join_date']
    for col in timestamp_cols:
        if col in new_records.columns:
            new_records[col] = new_records[col].apply(lambda x: None if pd.isna(x) else x)

    if not new_records.empty:
        logger.info(f"Inserting {len(new_records)} new records")
        upsert_df(new_records.drop(columns=['norm_key'], errors='ignore'), table_name, [business_key], conn)

    changed_mask = (
        ~merged[business_key + '_old'].isna() &
        merged[[f"{c}_new" for c in tracked_cols]].ne(merged[[f"{c}_old" for c in tracked_cols]]).any(axis=1)
    )
    changed = merged[changed_mask]

    if not changed.empty:
        logger.info(f"Updating {len(changed)} changed records")
        for idx, row in changed.iterrows():
            bk_raw = row[business_key + '_new'] if business_key + '_new' in row else row[business_key + '_old']
            bk = str(bk_raw).strip() if pd.notna(bk_raw) else None

            if bk is None or bk.lower() == 'nan':
                logger.warning(f"Skipping changed row with invalid business key: {bk_raw}")
                continue

            cursor.execute(
                f"""
                UPDATE {table_name}
                SET is_current = FALSE, effective_end = NOW()
                WHERE {business_key}::text = %s AND is_current = TRUE
                """,
                (bk,)
            )
            conn.commit()

            new_row = row[[c for c in row.index if '_new' in c]].rename(lambda x: x.replace('_new', ''))
            
            for col in timestamp_cols:
                if col in new_row.index and pd.isna(new_row[col]):
                    new_row[col] = None

            new_row['effective_start'] = datetime.now() if pd.isna(new_row.get('effective_start')) else new_row['effective_start']
            new_row['effective_end'] = 'infinity'
            new_row['is_current'] = True

            cols = ', '.join(new_row.index)
            placeholders = ', '.join(['%s'] * len(new_row))
            query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            try:
                cursor.execute(query, tuple(new_row.values))
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                logger.warning(f"Duplicate insert attempt for '{bk}' - skipping")
                conn.rollback()
            except Exception as e:
                conn.rollback()
                logger.error(f"Insert failed for '{bk}': {e}")
                raise

    logger.info(f"SCD Type 2 complete for {table_name}: {len(new_records)} new, {len(changed)} updates")

def load_all(extracted_data: dict, conn=None):
    """Full load orchestration: dimensions → fact → metadata."""
    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True

    try:
        logger.info("Loading dimensions with SCD Type 2...")
        if 'products' in extracted_data:
            handle_scd_type2(
                extracted_data['products'],
                'dim_product',
                'product_name',
                ['category', 'price', 'carbon_footprint_rating'],
                conn
            )

        if 'customers' in extracted_data:
            handle_scd_type2(
                extracted_data['customers'],
                'dim_customer',
                'email',
                ['customer_name', 'loyalty_level', 'join_date'],
                conn
            )

        logger.info("Loading fact table...")
        if 'sales' in extracted_data:
            fact_df = map_fact_foreign_keys(extracted_data['sales'], conn)
            if not fact_df.empty:
                fact_columns = [
                    'sale_id', 'date_id', 'product_id', 'customer_id',
                    'location_id', 'quantity_sold', 'revenue',
                    'carbon_savings', 'sale_timestamp'
                ]

                existing_cols = [c for c in fact_columns if c in fact_df.columns]
                fact_df = fact_df[existing_cols]

                logger.info(f"Preparing to upsert {len(fact_df)} rows with columns: {existing_cols}")

                if 'revenue' in fact_df.columns:
                    fact_df['revenue'] = fact_df['revenue'].fillna(0.00)
                if 'carbon_savings' in fact_df.columns:
                    fact_df['carbon_savings'] = fact_df['carbon_savings'].fillna(0.00)

                upsert_df(fact_df, 'fact_sales', ['sale_id'], conn)
            else:
                logger.warning("No valid fact rows after FK mapping")

        logger.info("Logging metadata...")
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO metadata_loads (load_timestamp, rows_loaded, status)
            VALUES (NOW(), %s, 'SUCCESS')
            """,
            (len(extracted_data.get('sales', pd.DataFrame())),)
        )
        conn.commit()

        logger.info("Load complete - all data committed")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Load failed: {e}")
        raise
    finally:
        if close_conn and conn:
            conn.close()

def map_fact_foreign_keys(df_sales: pd.DataFrame, conn) -> pd.DataFrame:
    """Map business strings to surrogate IDs from dimension tables."""
    cursor = conn.cursor()
    df = df_sales.copy()

    cursor.execute("SELECT date, date_id FROM dim_date")
    date_map = dict(cursor.fetchall())
    df['date_id'] = df['date'].map(date_map)
    df['date_id'] = df['date_id'].fillna(1)

    cursor.execute("SELECT product_name, product_id FROM dim_product WHERE is_current = TRUE")
    product_rows = cursor.fetchall()
    product_map = {str(name).strip().lower(): pid for name, pid in product_rows if name is not None}
    df['product_name_clean'] = df['product_name'].astype(str).str.strip().str.lower()
    df['product_id'] = df['product_name_clean'].map(product_map).fillna(1)

    cursor.execute("SELECT email, customer_id FROM dim_customer WHERE is_current = TRUE")
    customer_rows = cursor.fetchall()
    customer_map = {str(row[0]).lower().strip(): row[1] for row in customer_rows}
    df['customer_email_lower'] = df['customer_email'].astype(str).str.strip().str.lower()
    df['customer_id'] = df['customer_email_lower'].map(customer_map).fillna(1)

    cursor.execute("SELECT city, location_id FROM dim_location")
    location_rows = cursor.fetchall()
    location_map = {str(row[0]).lower().strip(): row[1] for row in location_rows}
    df['city_lower'] = df['city'].fillna('Unknown').astype(str).str.strip().str.lower()
    df['location_id'] = df['city_lower'].map(location_map).fillna(1)

    cursor.close()

    fk_cols = ['date_id', 'product_id', 'customer_id', 'location_id']
    missing = df[fk_cols].isna().any(axis=1)

    drop_cols = ['date', 'product_name', 'customer_email', 'city', 'customer_email_lower', 'city_lower', 'product_name_clean']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

    if 'quantity' in df.columns:
        df = df.rename(columns={'quantity': 'quantity_sold'})

    if missing.any():
        df = df[~missing]

    logger.info(f"Fact table ready with FKs mapped: {len(df)} rows")
    return df