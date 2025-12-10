# Research Notes - Northwind PowerBI Migration

## PowerBI Project Analysis

### Data Model
The PowerBI project uses an OData feed from the Northwind sample database. The data model consists of three main tables:

1. **Order_Details** - Core fact table with order line items
   - OrderID, ProductID, UnitPrice, Quantity, Discount
   - Expanded with Order, Customer, Employee, Shipper data
   - Calculated columns: Gross Revenue, Discount ($), Net Revenue, Days to Ship

2. **Product** - Product dimension
   - ProductID, ProductName, SupplierID, CategoryID
   - Joined with Categories for CategoryName

3. **Suppliers** - Supplier dimension
   - SupplierID, CompanyName, ContactName, etc.

### Relationships
- Order_Details.ProductID -> Product.ProductID
- Product.SupplierID -> Suppliers.SupplierID
- Date relationships for OrderDate, ShippedDate, HireDate

### Calculated Columns (DAX/M)
- Gross Revenue = UnitPrice * Quantity
- Discount ($) = Gross Revenue * Discount (%)
- Net Revenue = Gross Revenue - Discount ($)
- Days to Ship = ShippedDate - OrderDate

### Measures
- Orders = DISTINCTCOUNT(Order_Details[OrderID])
- Net Revenue per order = DIVIDE(SUM(Order_Details[Net Revenue]), Order_Details[Orders], BLANK())

## PostgreSQL Database Analysis

### Tables
| Table | Rows | Description |
|-------|------|-------------|
| order_details | 2,155 | Order line items |
| orders | 830 | Order headers |
| products | 77 | Product catalog |
| categories | 8 | Product categories |
| suppliers | 29 | Supplier info |
| customers | 91 | Customer info |
| employees | 9 | Employee info |
| shippers | 6 | Shipping companies |

### Key Differences from PowerBI Model
1. PowerBI denormalizes data in Power Query (M)
2. PostgreSQL maintains normalized structure
3. Need to create views in Snowflake to match PowerBI transformations

## Dashboard Pages

### Page 1: Overview (Dashboard)
**Filters:**
- Category Name, Product Name (dropdown)
- Country, City (dropdown)
- Title, Employee Name (dropdown)
- Date range slider (11/10/1996 - 27/12/1997)

**KPIs:**
- Sum of Gross Revenue: 774.4K
- Sum of Discount ($): 51.7K
- Sum of Net Revenue: 722.6K
- Orders: 474
- Sum of Quantity: 30.3K
- Average of Days to Ship: 8.39 (gauge with min=1, max=37)

**Visualizations:**
- Map: Net Revenue by Country and City
- Combo Chart: Total Orders Vs Gross Revenue by Month
- Horizontal Bar: Average Days to ship by Shipping Company

### Page 2: Category and Product
**Visualizations:**
- Top 5 Products by Orders (green horizontal bar)
- Bottom 5 Products by Orders (orange horizontal bar)
- Category and Product level Performance (pivot table)
- Unit in Stock and Unit on Order (pivot table)
- Unit in Stock by Category and Product (column chart)

### Page 3: Employees
**Visualizations:**
- Top 5 Employees by Orders (green horizontal bar)
- Bottom 5 Employees by Orders (orange horizontal bar)
- Title and Employee level Performance (pivot table)
- Net Revenue by Employee Title (waterfall chart)
- Net Revenue per order by Employee (column chart)

## Migration Strategy

### Data Migration
1. Export all tables from PostgreSQL
2. Create corresponding tables in Snowflake
3. Create views that replicate PowerBI transformations:
   - ORDER_DETAILS_VIEW: Joins all tables with calculated columns
   - PRODUCT_VIEW: Products with category info

### Streamlit App Structure
1. Three pages matching PowerBI
2. Sidebar filters replicating slicers
3. Plotly charts matching PowerBI visuals
4. Custom CSS for styling

### Validation Approach
1. Row count verification
2. Key metrics comparison (Gross Revenue, Net Revenue, Orders, etc.)
3. Visual comparison with PowerBI screenshots
