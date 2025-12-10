#!/usr/bin/env python3
"""
Migrate data from PostgreSQL Northwind database to Snowflake.
"""
import os
import psycopg2
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import toml

# Configuration
DB_SUFFIX = "478908"
SNOWFLAKE_DATABASE = f"NORTHWIND_{DB_SUFFIX}"
SNOWFLAKE_SCHEMA = "PUBLIC"

PG_CONFIG = {
    "host": "localhost",
    "port": 55432,
    "database": "northwind",
    "user": "postgres",
    "password": "postgres"
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
        account=sf_config["account"],
        user=sf_config["user"],
        private_key=private_key_bytes,
        role=sf_config["role"],
        warehouse=sf_config["warehouse"],
    )

def migrate_table(pg_conn, sf_conn, table_name, columns, create_sql):
    print(f"Migrating {table_name}...")
    
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT {columns} FROM {table_name}")
    rows = pg_cursor.fetchall()
    print(f"  Read {len(rows)} rows from PostgreSQL")
    
    sf_cursor = sf_conn.cursor()
    sf_cursor.execute(f"DROP TABLE IF EXISTS {table_name.upper()}")
    sf_cursor.execute(create_sql)
    
    if rows:
        placeholders = ", ".join(["%s"] * len(rows[0]))
        insert_sql = f"INSERT INTO {table_name.upper()} VALUES ({placeholders})"
        sf_cursor.executemany(insert_sql, rows)
    
    sf_cursor.execute(f"SELECT COUNT(*) FROM {table_name.upper()}")
    count = sf_cursor.fetchone()[0]
    print(f"  Wrote {count} rows to Snowflake")
    
    pg_cursor.close()
    sf_cursor.close()

def create_views(sf_conn):
    cursor = sf_conn.cursor()
    cursor.execute("""
    CREATE OR REPLACE VIEW ORDER_DETAILS_VIEW AS
    SELECT 
        od.ORDER_ID, od.PRODUCT_ID, od.UNIT_PRICE, od.QUANTITY, od.DISCOUNT,
        o.ORDER_DATE, o.SHIPPED_DATE, o.CUSTOMER_ID, o.EMPLOYEE_ID, o.SHIP_VIA,
        c.COMPANY_NAME AS CUSTOMER_COMPANY, c.CONTACT_NAME AS CUSTOMER_CONTACT,
        c.CONTACT_TITLE AS CUSTOMER_TITLE, c.CITY AS CUSTOMER_CITY, c.COUNTRY AS CUSTOMER_COUNTRY,
        e.LAST_NAME AS EMPLOYEE_LAST_NAME, e.FIRST_NAME AS EMPLOYEE_NAME,
        e.TITLE AS EMPLOYEE_TITLE, e.HIRE_DATE, e.CITY AS EMPLOYEE_CITY,
        s.COMPANY_NAME AS SHIPPING_COMPANY,
        p.PRODUCT_NAME, p.CATEGORY_ID, cat.CATEGORY_NAME,
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
    
    cursor.execute("""
    CREATE OR REPLACE VIEW PRODUCT_VIEW AS
    SELECT p.PRODUCT_ID, p.PRODUCT_NAME, p.SUPPLIER_ID, p.CATEGORY_ID,
           p.UNIT_PRICE, p.UNITS_IN_STOCK, p.UNITS_ON_ORDER,
           c.CATEGORY_NAME, c.DESCRIPTION AS CATEGORY_DESCRIPTION
    FROM PRODUCTS p
    LEFT JOIN CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
    """)
    print("Created PRODUCT_VIEW")
    cursor.close()

def main():
    print("Starting migration...")
    
    pg_conn = psycopg2.connect(**PG_CONFIG)
    print("Connected to PostgreSQL")
    
    sf_conn = get_snowflake_connection()
    print("Connected to Snowflake")
    
    cursor = sf_conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    print(f"Created database {SNOWFLAKE_DATABASE}")
    cursor.close()
    
    tables = [
        ("categories", "category_id, category_name, description",
         "CREATE TABLE CATEGORIES (CATEGORY_ID INT, CATEGORY_NAME VARCHAR, DESCRIPTION TEXT)"),
        ("customers", "customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax",
         "CREATE TABLE CUSTOMERS (CUSTOMER_ID VARCHAR, COMPANY_NAME VARCHAR, CONTACT_NAME VARCHAR, CONTACT_TITLE VARCHAR, ADDRESS VARCHAR, CITY VARCHAR, REGION VARCHAR, POSTAL_CODE VARCHAR, COUNTRY VARCHAR, PHONE VARCHAR, FAX VARCHAR)"),
        ("employees", "employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, notes, reports_to, photo_path",
         "CREATE TABLE EMPLOYEES (EMPLOYEE_ID INT, LAST_NAME VARCHAR, FIRST_NAME VARCHAR, TITLE VARCHAR, TITLE_OF_COURTESY VARCHAR, BIRTH_DATE DATE, HIRE_DATE DATE, ADDRESS VARCHAR, CITY VARCHAR, REGION VARCHAR, POSTAL_CODE VARCHAR, COUNTRY VARCHAR, HOME_PHONE VARCHAR, EXTENSION VARCHAR, NOTES TEXT, REPORTS_TO INT, PHOTO_PATH VARCHAR)"),
        ("suppliers", "supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage",
         "CREATE TABLE SUPPLIERS (SUPPLIER_ID INT, COMPANY_NAME VARCHAR, CONTACT_NAME VARCHAR, CONTACT_TITLE VARCHAR, ADDRESS VARCHAR, CITY VARCHAR, REGION VARCHAR, POSTAL_CODE VARCHAR, COUNTRY VARCHAR, PHONE VARCHAR, FAX VARCHAR, HOMEPAGE TEXT)"),
        ("shippers", "shipper_id, company_name, phone",
         "CREATE TABLE SHIPPERS (SHIPPER_ID INT, COMPANY_NAME VARCHAR, PHONE VARCHAR)"),
        ("products", "product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued",
         "CREATE TABLE PRODUCTS (PRODUCT_ID INT, PRODUCT_NAME VARCHAR, SUPPLIER_ID INT, CATEGORY_ID INT, QUANTITY_PER_UNIT VARCHAR, UNIT_PRICE FLOAT, UNITS_IN_STOCK INT, UNITS_ON_ORDER INT, REORDER_LEVEL INT, DISCONTINUED INT)"),
        ("orders", "order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country",
         "CREATE TABLE ORDERS (ORDER_ID INT, CUSTOMER_ID VARCHAR, EMPLOYEE_ID INT, ORDER_DATE DATE, REQUIRED_DATE DATE, SHIPPED_DATE DATE, SHIP_VIA INT, FREIGHT FLOAT, SHIP_NAME VARCHAR, SHIP_ADDRESS VARCHAR, SHIP_CITY VARCHAR, SHIP_REGION VARCHAR, SHIP_POSTAL_CODE VARCHAR, SHIP_COUNTRY VARCHAR)"),
        ("order_details", "order_id, product_id, unit_price, quantity, discount",
         "CREATE TABLE ORDER_DETAILS (ORDER_ID INT, PRODUCT_ID INT, UNIT_PRICE FLOAT, QUANTITY INT, DISCOUNT FLOAT)"),
    ]
    
    for table_name, columns, create_sql in tables:
        migrate_table(pg_conn, sf_conn, table_name, columns, create_sql)
    
    create_views(sf_conn)
    
    print("Migration completed successfully!")
    
    pg_conn.close()
    sf_conn.close()

if __name__ == "__main__":
    main()
