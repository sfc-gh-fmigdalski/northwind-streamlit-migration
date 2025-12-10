#!/usr/bin/env python3
"""
Verify data migration between PostgreSQL and Snowflake.
"""
import psycopg2
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os
import pandas as pd

DB_SUFFIX = "478908"
SNOWFLAKE_DATABASE = f"NORTHWIND_{DB_SUFFIX}"

PG_CONFIG = {
    "host": "localhost",
    "port": 55432,
    "database": "northwind",
    "user": "postgres",
    "password": "postgres"
}

def get_snowflake_connection():
    import tomllib
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    
    sf_config = config["snowvation_playground"]
    private_key_path = sf_config["private_key_file"]
    with open(private_key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        account=sf_config["account"],
        user=sf_config["user"],
        private_key=private_key_bytes,
        role=sf_config["role"],
        warehouse=sf_config["warehouse"],
        database=SNOWFLAKE_DATABASE,
        schema="PUBLIC"
    )

def verify_row_counts():
    """Verify row counts match between systems."""
    print("
=== Row Count Verification ===")
    
    pg_conn = psycopg2.connect(**PG_CONFIG)
    sf_conn = get_snowflake_connection()
    
    tables = ["categories", "customers", "employees", "suppliers", 
              "shippers", "products", "orders", "order_details"]
    
    all_match = True
    for table in tables:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        pg_count = pg_cursor.fetchone()[0]
        
        sf_cursor = sf_conn.cursor()
        sf_cursor.execute(f"SELECT COUNT(*) FROM {table.upper()}")
        sf_count = sf_cursor.fetchone()[0]
        
        match = "OK" if pg_count == sf_count else "MISMATCH"
        if pg_count != sf_count:
            all_match = False
        print(f"  {table}: PostgreSQL={pg_count}, Snowflake={sf_count} [{match}]")
    
    pg_conn.close()
    sf_conn.close()
    return all_match

def verify_key_metrics():
    """Verify key business metrics match."""
    print("
=== Key Metrics Verification ===")
    
    pg_conn = psycopg2.connect(**PG_CONFIG)
    sf_conn = get_snowflake_connection()
    
    # PostgreSQL metrics
    pg_query = """
    SELECT 
        SUM(od.unit_price * od.quantity) as gross_revenue,
        SUM(od.unit_price * od.quantity * od.discount) as discount,
        SUM(od.unit_price * od.quantity - od.unit_price * od.quantity * od.discount) as net_revenue,
        COUNT(DISTINCT od.order_id) as orders,
        SUM(od.quantity) as total_quantity
    FROM order_details od
    """
    pg_df = pd.read_sql(pg_query, pg_conn)
    
    # Snowflake metrics
    sf_query = """
    SELECT 
        SUM(GROSS_REVENUE) as gross_revenue,
        SUM(DISCOUNT_AMOUNT) as discount,
        SUM(NET_REVENUE) as net_revenue,
        COUNT(DISTINCT ORDER_ID) as orders,
        SUM(QUANTITY) as total_quantity
    FROM ORDER_DETAILS_VIEW
    """
    sf_df = pd.read_sql(sf_query, sf_conn)
    
    print(f"  Gross Revenue: PG={pg_df['gross_revenue'].iloc[0]:.2f}, SF={sf_df['gross_revenue'].iloc[0]:.2f}")
    print(f"  Discount: PG={pg_df['discount'].iloc[0]:.2f}, SF={sf_df['discount'].iloc[0]:.2f}")
    print(f"  Net Revenue: PG={pg_df['net_revenue'].iloc[0]:.2f}, SF={sf_df['net_revenue'].iloc[0]:.2f}")
    print(f"  Orders: PG={pg_df['orders'].iloc[0]}, SF={sf_df['orders'].iloc[0]}")
    print(f"  Quantity: PG={pg_df['total_quantity'].iloc[0]}, SF={sf_df['total_quantity'].iloc[0]}")
    
    pg_conn.close()
    sf_conn.close()

def main():
    print("Starting verification...")
    
    row_match = verify_row_counts()
    verify_key_metrics()
    
    print("
=== Verification Complete ===")
    if row_match:
        print("All row counts match!")
    else:
        print("WARNING: Some row counts do not match!")

if __name__ == "__main__":
    main()
