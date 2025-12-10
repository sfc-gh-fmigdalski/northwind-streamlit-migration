#!/usr/bin/env python3
"""Verify data migration between PostgreSQL and Snowflake."""
import os
import psycopg2
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import toml

DB_SUFFIX = "478908"
SNOWFLAKE_DATABASE = f"NORTHWIND_{DB_SUFFIX}"

PG_CONFIG = {
    "host": "localhost", "port": 55432,
    "database": "northwind", "user": "postgres", "password": "postgres"
}

def get_snowflake_connection():
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    config = toml.load(config_path)
    sf_config = config["snowvation_playground"]
    
    private_key_path = sf_config["private_key_file"]
    if not private_key_path.startswith("/home"):
        private_key_path = os.path.expanduser("~" + private_key_path)
    
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
        account=sf_config["account"], user=sf_config["user"],
        private_key=private_key_bytes, role=sf_config["role"],
        warehouse=sf_config["warehouse"], database=SNOWFLAKE_DATABASE, schema="PUBLIC"
    )

def main():
    print("Starting verification...")
    
    pg_conn = psycopg2.connect(**PG_CONFIG)
    sf_conn = get_snowflake_connection()
    
    print("=== Row Count Verification ===")
    tables = ["categories", "customers", "employees", "suppliers", "shippers", "products", "orders", "order_details"]
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
    
    print("=== Key Metrics Verification ===")
    
    # PostgreSQL metrics
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT 
            SUM(unit_price * quantity) as gross_revenue,
            SUM(unit_price * quantity * discount) as discount,
            SUM(unit_price * quantity - unit_price * quantity * discount) as net_revenue,
            COUNT(DISTINCT order_id) as orders,
            SUM(quantity) as total_quantity
        FROM order_details
    """)
    pg_result = pg_cursor.fetchone()
    
    # Snowflake metrics
    sf_cursor = sf_conn.cursor()
    sf_cursor.execute("""
        SELECT 
            SUM(GROSS_REVENUE) as gross_revenue,
            SUM(DISCOUNT_AMOUNT) as discount,
            SUM(NET_REVENUE) as net_revenue,
            COUNT(DISTINCT ORDER_ID) as orders,
            SUM(QUANTITY) as total_quantity
        FROM ORDER_DETAILS_VIEW
    """)
    sf_result = sf_cursor.fetchone()
    
    metrics = ["Gross Revenue", "Discount", "Net Revenue", "Orders", "Quantity"]
    for i, metric in enumerate(metrics):
        pg_val = float(pg_result[i]) if pg_result[i] else 0
        sf_val = float(sf_result[i]) if sf_result[i] else 0
        match = "OK" if abs(pg_val - sf_val) < 0.01 else "MISMATCH"
        print(f"  {metric}: PostgreSQL={pg_val:.2f}, Snowflake={sf_val:.2f} [{match}]")
    
    print("=== Verification Complete ===")
    if all_match:
        print("All row counts match!")
    else:
        print("WARNING: Some row counts do not match!")
    
    pg_conn.close()
    sf_conn.close()

if __name__ == "__main__":
    main()
