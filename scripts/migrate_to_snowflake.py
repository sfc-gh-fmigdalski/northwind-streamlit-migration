#!/usr/bin/env python3
"""
Migrate data from PostgreSQL Northwind database to Snowflake.
"""
import os
import psycopg2
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import pandas as pd

# Configuration
DB_SUFFIX = "478908"
SNOWFLAKE_DATABASE = f"NORTHWIND_{DB_SUFFIX}"
SNOWFLAKE_SCHEMA = "PUBLIC"

# PostgreSQL connection settings
PG_CONFIG = {
    "host": "localhost",
    "port": 55432,
    "database": "northwind",
    "user": "postgres",
    "password": "postgres"
}

def get_snowflake_connection():
    """Create Snowflake connection using JWT authentication."""
    import tomllib
    
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    
    sf_config = config["snowvation_playground"]
    
    # Load private key
    private_key_path = sf_config["private_key_file"]
    with open(private_key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    
    # Get private key bytes
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
    )

def create_snowflake_database(sf_conn):
    """Create Snowflake database and schema."""
    cursor = sf_conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        print(f"Created database {SNOWFLAKE_DATABASE}")
    finally:
        cursor.close()

def migrate_table(pg_conn, sf_conn, table_name, create_sql):
    """Migrate a single table from PostgreSQL to Snowflake."""
    print(f"Migrating {table_name}...")
    
    # Read from PostgreSQL
    df = pd.read_sql(f"SELECT * FROM {table_name}", pg_conn)
    print(f"  Read {len(df)} rows from PostgreSQL")
    
    # Create table in Snowflake
    sf_cursor = sf_conn.cursor()
    try:
        sf_cursor.execute(f"DROP TABLE IF EXISTS {table_name.upper()}")
        sf_cursor.execute(create_sql)
        
        # Write to Snowflake using write_pandas
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            sf_conn, df, table_name.upper(), database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA
        )
        print(f"  Wrote {nrows} rows to Snowflake")
    finally:
        sf_cursor.close()

def create_views(sf_conn):
    """Create views that replicate PowerBI transformations."""
    cursor = sf_conn.cursor()
    try:
        # Create ORDER_DETAILS_VIEW with calculated columns
        cursor.execute("""
        CREATE OR REPLACE VIEW ORDER_DETAILS_VIEW AS
        SELECT 
            od.ORDER_ID,
            od.PRODUCT_ID,
            od.UNIT_PRICE,
            od.QUANTITY,
            od.DISCOUNT,
            o.ORDER_DATE,
            o.SHIPPED_DATE,
            o.CUSTOMER_ID,
            o.EMPLOYEE_ID,
            o.SHIP_VIA,
            c.COMPANY_NAME AS CUSTOMER_COMPANY,
            c.CONTACT_NAME AS CUSTOMER_CONTACT,
            c.CONTACT_TITLE AS CUSTOMER_TITLE,
            c.CITY AS CUSTOMER_CITY,
            c.COUNTRY AS CUSTOMER_COUNTRY,
            e.LAST_NAME AS EMPLOYEE_LAST_NAME,
            e.FIRST_NAME AS EMPLOYEE_NAME,
            e.TITLE AS EMPLOYEE_TITLE,
            e.HIRE_DATE,
            e.CITY AS EMPLOYEE_CITY,
            s.COMPANY_NAME AS SHIPPING_COMPANY,
            p.PRODUCT_NAME,
            p.CATEGORY_ID,
            cat.CATEGORY_NAME,
            -- Calculated columns (matching PowerBI)
            (od.UNIT_PRICE * od.QUANTITY) AS GROSS_REVENUE,
            (od.UNIT_PRICE * od.QUANTITY * od.DISCOUNT) AS DISCOUNT_AMOUNT,
            (od.UNIT_PRICE * od.QUANTITY) - (od.UNIT_PRICE * od.QUANTITY * od.DISCOUNT) AS NET_REVENUE,
            DATEDIFF(day, o.ORDER_DATE, o.SHIPPED_DATE) AS DAYS_TO_SHIP
        FROM ORDER_DETAILS od
        JOIN ORDERS o ON od.ORDER_ID = o.ORDER_ID
        LEFT JOIN CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN EMPLOYEES e ON o.EMPLOYEE_ID = e.EMPLOYEE_ID
        LEFT JOIN SHIPPERS s ON o.SHIP_VIA = s.SHIPPER_ID
        LEFT JOIN PRODUCTS p ON od.PRODUCT_ID = p.PRODUCT_ID
        LEFT JOIN CATEGORIES cat ON p.CATEGORY_ID = cat.CATEGORY_ID
        """)
        print("Created ORDER_DETAILS_VIEW")
        
        # Create PRODUCT_VIEW with category info
        cursor.execute("""
        CREATE OR REPLACE VIEW PRODUCT_VIEW AS
        SELECT 
            p.PRODUCT_ID,
            p.PRODUCT_NAME,
            p.SUPPLIER_ID,
            p.CATEGORY_ID,
            p.UNIT_PRICE,
            p.UNITS_IN_STOCK,
            p.UNITS_ON_ORDER,
            c.CATEGORY_NAME,
            c.DESCRIPTION AS CATEGORY_DESCRIPTION
        FROM PRODUCTS p
        LEFT JOIN CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
        """)
        print("Created PRODUCT_VIEW")
        
    finally:
        cursor.close()

def main():
    print("Starting migration...")
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**PG_CONFIG)
    print("Connected to PostgreSQL")
    
    # Connect to Snowflake
    sf_conn = get_snowflake_connection()
    print("Connected to Snowflake")
    
    try:
        # Create database
        create_snowflake_database(sf_conn)
        
        # Define table schemas
        tables = {
            "categories": """
                CREATE TABLE CATEGORIES (
                    CATEGORY_ID INTEGER PRIMARY KEY,
                    CATEGORY_NAME VARCHAR(255),
                    DESCRIPTION TEXT,
                    PICTURE BINARY
                )
            """,
            "customers": """
                CREATE TABLE CUSTOMERS (
                    CUSTOMER_ID VARCHAR(10) PRIMARY KEY,
                    COMPANY_NAME VARCHAR(255),
                    CONTACT_NAME VARCHAR(255),
                    CONTACT_TITLE VARCHAR(255),
                    ADDRESS VARCHAR(255),
                    CITY VARCHAR(255),
                    REGION VARCHAR(255),
                    POSTAL_CODE VARCHAR(255),
                    COUNTRY VARCHAR(255),
                    PHONE VARCHAR(255),
                    FAX VARCHAR(255)
                )
            """,
            "employees": """
                CREATE TABLE EMPLOYEES (
                    EMPLOYEE_ID INTEGER PRIMARY KEY,
                    LAST_NAME VARCHAR(255),
                    FIRST_NAME VARCHAR(255),
                    TITLE VARCHAR(255),
                    TITLE_OF_COURTESY VARCHAR(255),
                    BIRTH_DATE DATE,
                    HIRE_DATE DATE,
                    ADDRESS VARCHAR(255),
                    CITY VARCHAR(255),
                    REGION VARCHAR(255),
                    POSTAL_CODE VARCHAR(255),
                    COUNTRY VARCHAR(255),
                    HOME_PHONE VARCHAR(255),
                    EXTENSION VARCHAR(255),
                    PHOTO BINARY,
                    NOTES TEXT,
                    REPORTS_TO INTEGER,
                    PHOTO_PATH VARCHAR(255)
                )
            """,
            "suppliers": """
                CREATE TABLE SUPPLIERS (
                    SUPPLIER_ID INTEGER PRIMARY KEY,
                    COMPANY_NAME VARCHAR(255),
                    CONTACT_NAME VARCHAR(255),
                    CONTACT_TITLE VARCHAR(255),
                    ADDRESS VARCHAR(255),
                    CITY VARCHAR(255),
                    REGION VARCHAR(255),
                    POSTAL_CODE VARCHAR(255),
                    COUNTRY VARCHAR(255),
                    PHONE VARCHAR(255),
                    FAX VARCHAR(255),
                    HOMEPAGE TEXT
                )
            """,
            "shippers": """
                CREATE TABLE SHIPPERS (
                    SHIPPER_ID INTEGER PRIMARY KEY,
                    COMPANY_NAME VARCHAR(255),
                    PHONE VARCHAR(255)
                )
            """,
            "products": """
                CREATE TABLE PRODUCTS (
                    PRODUCT_ID INTEGER PRIMARY KEY,
                    PRODUCT_NAME VARCHAR(255),
                    SUPPLIER_ID INTEGER,
                    CATEGORY_ID INTEGER,
                    QUANTITY_PER_UNIT VARCHAR(255),
                    UNIT_PRICE FLOAT,
                    UNITS_IN_STOCK INTEGER,
                    UNITS_ON_ORDER INTEGER,
                    REORDER_LEVEL INTEGER,
                    DISCONTINUED INTEGER
                )
            """,
            "orders": """
                CREATE TABLE ORDERS (
                    ORDER_ID INTEGER PRIMARY KEY,
                    CUSTOMER_ID VARCHAR(10),
                    EMPLOYEE_ID INTEGER,
                    ORDER_DATE DATE,
                    REQUIRED_DATE DATE,
                    SHIPPED_DATE DATE,
                    SHIP_VIA INTEGER,
                    FREIGHT FLOAT,
                    SHIP_NAME VARCHAR(255),
                    SHIP_ADDRESS VARCHAR(255),
                    SHIP_CITY VARCHAR(255),
                    SHIP_REGION VARCHAR(255),
                    SHIP_POSTAL_CODE VARCHAR(255),
                    SHIP_COUNTRY VARCHAR(255)
                )
            """,
            "order_details": """
                CREATE TABLE ORDER_DETAILS (
                    ORDER_ID INTEGER,
                    PRODUCT_ID INTEGER,
                    UNIT_PRICE FLOAT,
                    QUANTITY INTEGER,
                    DISCOUNT FLOAT,
                    PRIMARY KEY (ORDER_ID, PRODUCT_ID)
                )
            """
        }
        
        # Migrate each table
        for table_name, create_sql in tables.items():
            migrate_table(pg_conn, sf_conn, table_name, create_sql)
        
        # Create views
        create_views(sf_conn)
        
        print("
Migration completed successfully!")
        
    finally:
        pg_conn.close()
        sf_conn.close()

if __name__ == "__main__":
    main()
