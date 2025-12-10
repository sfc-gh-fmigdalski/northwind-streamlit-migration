# Northwind PowerBI to Streamlit Migration

Migration of the Northwind PowerBI dashboard to a Streamlit application backed by Snowflake.

## Overview

This project migrates a PowerBI dashboard (Northwind Traders data) to a Streamlit web application with Snowflake as the data backend.

### Source System
- **PowerBI Project**: Located in northwind-powerbi directory
- **Legacy Database**: PostgreSQL (Northwind database)
  - Host: localhost, Port: 55432, Database: northwind, User: postgres

### Target System
- **Application**: Streamlit
- **Database**: Snowflake (using snowvation_playground connection)
- **Database Suffix**: 478908

## Database Schema

### Source Tables (PostgreSQL)
| Table | Rows |
|-------|------|
| order_details | 2,155 |
| orders | 830 |
| products | 77 |
| categories | 8 |
| suppliers | 29 |
| customers | 91 |
| employees | 9 |
| shippers | 6 |

### Target Database (Snowflake)
Database name: NORTHWIND_478908

## PowerBI Dashboard Structure

The dashboard has 3 pages:
1. **Overview** - Main KPIs, map, combo charts, and gauges
2. **Category and Product** - Product analytics with pivot tables
3. **Employees** - Employee performance analysis

### Key Metrics
- Gross Revenue = UnitPrice x Quantity
- Discount = Gross Revenue x Discount Percentage
- Net Revenue = Gross Revenue - Discount
- Days to Ship = ShippedDate - OrderDate

## Setup and Usage

### Installation
cd northwind-streamlit-migration
uv sync

### Migration
uv run python scripts/migrate_to_snowflake.py

### Verify Migration
uv run python scripts/verify_migration.py

### Run Streamlit App (Local)
uv run streamlit run streamlit_app/app.py

**Local URL**: http://localhost:8510

## Snowflake Deployment

The app is deployed to Snowflake as Streamlit in Snowflake (SiS).

**Snowflake Streamlit App URL**:
https://app.snowflake.com/sfengineering/ai_powered_playground/#/streamlit-apps/NORTHWIND_478908.PUBLIC.NORTHWIND_DASHBOARD

## Verification Results

### Row Count Verification
All tables verified with matching row counts between PostgreSQL and Snowflake.

### Key Metrics Verification
| Metric | PostgreSQL | Snowflake | Match |
|--------|------------|-----------|-------|
| Gross Revenue | 1,354,458.59 | 1,354,458.59 | OK |
| Discount | 88,665.55 | 88,665.55 | OK |
| Net Revenue | 1,265,793.04 | 1,265,793.04 | OK |
| Orders | 830 | 830 | OK |
| Quantity | 51,317 | 51,317 | OK |
