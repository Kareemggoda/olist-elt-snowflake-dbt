import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime


def get_snowflake_config():
    return {
        "account":   os.environ.get("SNOWFLAKE_ACCOUNT",   "iw12052.eu-west-2.aws"),
        "user":      os.environ.get("SNOWFLAKE_USER",      "KAREEM"),
        "password":  os.environ.get("SNOWFLAKE_PASSWORD",  "Ka@01098935452"),  # يقرا أوتوماتيك من ملف الـ .env للأمان
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "olist_wh"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE",  "olist_db"),
        "role":      os.environ.get("SNOWFLAKE_ROLE",      "transformer"),
    }


FILES = {
    "ORDERS":                       "olist_orders_dataset.csv",
    "CUSTOMERS":                    "olist_customers_dataset.csv",
    "PRODUCTS":                     "olist_products_dataset.csv",
    "SELLERS":                      "olist_sellers_dataset.csv",
    "ORDER_ITEMS":                  "olist_order_items_dataset.csv",
    "ORDER_PAYMENTS":               "olist_order_payments_dataset.csv",
    "ORDER_REVIEWS":                "olist_order_reviews_dataset.csv",
    "GEOLOCATION":                  "olist_geolocation_dataset.csv",
    "PRODUCT_CATEGORY_TRANSLATION": "product_category_name_translation.csv",
}

DATA_DIR = os.environ.get(
    "DATA_DIR",
    os.path.join(os.path.dirname(__file__), "..", "data")
)


def get_connection(config):
    print("  Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema="RAW",
        role=config["role"],
    )
    print(f"  Connected → {config['account']} / {config['database']}.RAW")
    return conn


def load_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
    df.columns = [c.upper() for c in df.columns]
    df = df.replace(r'^\s*$', None, regex=True)
    df["_LOAD_TIMESTAMP"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if "product_category_name_translation" in filepath.lower():
        exact_cols = ['PRODUCT_CATEGORY_NAME', 'PRODUCT_CATEGORY_NAME_ENGLISH', '_LOAD_TIMESTAMP']
        df = df[[col for col in exact_cols if col in df.columns]]
        print("    → Filtered DataFrame columns to match PRODUCT_CATEGORY_TRANSLATION DDL exactly.")
        
    return df


def upload_table(conn, config: dict, table_name: str, df: pd.DataFrame):
    cursor = conn.cursor()

    cursor.execute(f"USE DATABASE {config['database']}")
    cursor.execute(f"USE SCHEMA RAW")
    cursor.execute(f"TRUNCATE TABLE IF EXISTS RAW.{table_name}")
    print(f"    Truncated RAW.{table_name}")

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        schema="RAW",
        auto_create_table=False,
        overwrite=False,
    )

    if success:
        print(f"RAW.{table_name:<40} {nrows:>10,} rows")
    else:
        raise RuntimeError(f"write_pandas failed for {table_name}")

    cursor.close()


def verify_counts(conn, config: dict):
    print("\n  ── Row count verification ──────────────────────")
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {config['database']}")
    for table in FILES.keys():
        cursor.execute(f"SELECT COUNT(*) FROM RAW.{table}")
        count = cursor.fetchone()[0]
        print(f"  RAW.{table:<42} {count:>10,} rows")
    cursor.close()
    print("  ────────────────────────────────────────────────\n")


def main():
    start = datetime.utcnow()
    print(f"\n{'='*55}")
    print(f"  Olist → Snowflake Ingestion  |  {start:%Y-%m-%d %H:%M} UTC")
    print(f"{'='*55}\n")

    try:
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        load_dotenv(env_path)
        print("  Loaded .env file")
    except ImportError:
        print("  python-dotenv not installed — using system env vars")

    config = get_snowflake_config()

    if not config["password"]:
        raise ValueError(
            "SNOWFLAKE_PASSWORD is empty! "
            "Check your .env file."
        )

    conn = get_connection(config)

    try:
        for table_name, csv_file in FILES.items():
            filepath = os.path.join(DATA_DIR, csv_file)

            if not os.path.exists(filepath):
                print(f"Not found — skipping: {filepath}")
                continue

            print(f"\n  Loading {csv_file} ...")
            df = load_csv(filepath)
            
            print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            upload_table(conn, config, table_name, df)

        verify_counts(conn, config)

    finally:
        conn.close()
        print("  Connection closed.")

    elapsed = (datetime.utcnow() - start).seconds
    print(f"All done in {elapsed}s\n")


if __name__ == "__main__":
    main()