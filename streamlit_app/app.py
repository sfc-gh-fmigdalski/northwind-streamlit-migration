#!/usr/bin/env python3
"""
Northwind Dashboard - Streamlit Application
Replicates the PowerBI dashboard with Snowflake backend.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os
from datetime import datetime, date

# Page config
st.set_page_config(
    page_title="Northwind Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp {
        background-color: #f5f5f5;
    }
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 5px;
        text-align: center;
    }
    .metric-value {
        font-size: 36px;
        font-weight: bold;
        color: #1f4e79;
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .section-header {
        background: linear-gradient(90deg, #1f4e79 0%, #2e75b6 100%);
        color: white;
        padding: 10px 15px;
        border-radius: 5px 5px 0 0;
        margin-bottom: 0;
    }
</style>
""", unsafe_allow_html=True)

# Database configuration
DB_SUFFIX = "478908"
SNOWFLAKE_DATABASE = f"NORTHWIND_{DB_SUFFIX}"

@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection."""
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

@st.cache_data(ttl=300)
def load_data():
    """Load data from Snowflake."""
    conn = get_snowflake_connection()
    
    query = """
    SELECT * FROM ORDER_DETAILS_VIEW
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    
    # Convert dates
    for col in ["order_date", "shipped_date", "hire_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    return df

@st.cache_data(ttl=300)
def load_products():
    """Load products data."""
    conn = get_snowflake_connection()
    query = "SELECT * FROM PRODUCT_VIEW"
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    return df

def format_number(num, prefix=""):
    """Format numbers with K/M suffix."""
    if num >= 1_000_000:
        return f"{prefix}{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{prefix}{num/1_000:.1f}K"
    else:
        return f"{prefix}{num:.0f}"

def render_kpi_card(value, label, sparkline_data=None):
    """Render a KPI card with optional sparkline."""
    formatted = format_number(value)
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{formatted}</div>
        <div class="metric-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

def render_sidebar_filters(df):
    """Render sidebar filters."""
    st.sidebar.markdown("### Filters")
    
    # Category filter
    categories = ["All"] + sorted(df["category_name"].dropna().unique().tolist())
    selected_category = st.sidebar.selectbox("Category Name", categories)
    
    # Product filter
    if selected_category != "All":
        products = ["All"] + sorted(df[df["category_name"] == selected_category]["product_name"].dropna().unique().tolist())
    else:
        products = ["All"] + sorted(df["product_name"].dropna().unique().tolist())
    selected_product = st.sidebar.selectbox("Product Name", products)
    
    # Country filter
    countries = ["All"] + sorted(df["customer_country"].dropna().unique().tolist())
    selected_country = st.sidebar.selectbox("Country", countries)
    
    # Employee filter
    employees = ["All"] + sorted(df["employee_name"].dropna().unique().tolist())
    selected_employee = st.sidebar.selectbox("Employee Name", employees)
    
    # Date range
    min_date = df["order_date"].min().date()
    max_date = df["order_date"].max().date()
    date_range = st.sidebar.date_input(
        "Order Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    return {
        "category": selected_category,
        "product": selected_product,
        "country": selected_country,
        "employee": selected_employee,
        "date_range": date_range
    }

def apply_filters(df, filters):
    """Apply filters to dataframe."""
    filtered = df.copy()
    
    if filters["category"] != "All":
        filtered = filtered[filtered["category_name"] == filters["category"]]
    
    if filters["product"] != "All":
        filtered = filtered[filtered["product_name"] == filters["product"]]
    
    if filters["country"] != "All":
        filtered = filtered[filtered["customer_country"] == filters["country"]]
    
    if filters["employee"] != "All":
        filtered = filtered[filtered["employee_name"] == filters["employee"]]
    
    if len(filters["date_range"]) == 2:
        start_date, end_date = filters["date_range"]
        filtered = filtered[
            (filtered["order_date"].dt.date >= start_date) &
            (filtered["order_date"].dt.date <= end_date)
        ]
    
    return filtered

def overview_page(df, filters):
    """Render Overview page."""
    filtered = apply_filters(df, filters)
    
    # KPI Row
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    gross_revenue = filtered["gross_revenue"].sum()
    discount = filtered["discount_amount"].sum()
    net_revenue = filtered["net_revenue"].sum()
    orders = filtered["order_id"].nunique()
    quantity = filtered["quantity"].sum()
    avg_days_ship = filtered["days_to_ship"].mean() if not filtered["days_to_ship"].isna().all() else 0
    
    with col1:
        st.metric("Sum of Gross Revenue", format_number(gross_revenue))
    with col2:
        st.metric("Sum of Discount ($)", format_number(discount))
    with col3:
        st.metric("Sum of Net Revenue", format_number(net_revenue))
    with col4:
        st.metric("Orders", f"{orders:,}")
    with col5:
        st.metric("Sum of Quantity", format_number(quantity))
    with col6:
        st.metric("Avg Days to Ship", f"{avg_days_ship:.2f}")
    
    st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class="section-header">Net Revenue by Country and City</div>", unsafe_allow_html=True)
        country_revenue = filtered.groupby("customer_country")["net_revenue"].sum().reset_index()
        fig = px.choropleth(
            country_revenue,
            locations="customer_country",
            locationmode="country names",
            color="net_revenue",
            color_continuous_scale="Blues",
            title=""
        )
        fig.update_layout(
            geo=dict(showframe=False, showcoastlines=True),
            margin=dict(l=0, r=0, t=0, b=0),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("<div class="section-header">Total Orders Vs Gross Revenue by Month</div>", unsafe_allow_html=True)
        monthly = filtered.groupby(filtered["order_date"].dt.to_period("M")).agg({
            "order_id": "nunique",
            "gross_revenue": "sum"
        }).reset_index()
        monthly["order_date"] = monthly["order_date"].astype(str)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=monthly["order_date"],
            y=monthly["gross_revenue"],
            name="Gross Revenue",
            marker_color="#2e75b6"
        ))
        fig.add_trace(go.Scatter(
            x=monthly["order_date"],
            y=monthly["order_id"] * 1000,
            name="Orders",
            mode="lines+markers",
            yaxis="y2",
            line=dict(color="#1f4e79", width=2)
        ))
        fig.update_layout(
            yaxis=dict(title="Gross Revenue"),
            yaxis2=dict(title="Orders", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=400,
            margin=dict(l=50, r=50, t=30, b=50)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Bottom chart
    st.markdown("<div class="section-header">Average Days to Ship by Shipping Company</div>", unsafe_allow_html=True)
    shipping = filtered.groupby("shipping_company")["days_to_ship"].mean().reset_index()
    shipping = shipping.sort_values("days_to_ship", ascending=True)
    
    fig = px.bar(
        shipping,
        x="days_to_ship",
        y="shipping_company",
        orientation="h",
        color_discrete_sequence=["#2e75b6"]
    )
    fig.update_layout(
        xaxis_title="Average Days to Ship",
        yaxis_title="",
        height=250,
        margin=dict(l=150, r=50, t=30, b=50)
    )
    fig.update_traces(texttemplate="%{x:.2f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

def category_product_page(df, filters):
    """Render Category and Product page."""
    filtered = apply_filters(df, filters)
    products_df = load_products()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class="section-header">Top/Bottom 5 Products by Orders</div>", unsafe_allow_html=True)
        
        product_orders = filtered.groupby("product_name")["order_id"].nunique().reset_index()
        product_orders.columns = ["Product Name", "Orders"]
        
        # Top 5
        st.markdown("**Top 5 Products by order**")
        top5 = product_orders.nlargest(5, "Orders")
        fig = px.bar(top5, x="Orders", y="Product Name", orientation="h", color_discrete_sequence=["#70ad47"])
        fig.update_layout(height=200, margin=dict(l=150, r=30, t=10, b=30), yaxis=dict(autorange="reversed"))
        fig.update_traces(texttemplate="%{x}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
        
        # Bottom 5
        st.markdown("**Bottom 5 Products by order**")
        bottom5 = product_orders.nsmallest(5, "Orders")
        fig = px.bar(bottom5, x="Orders", y="Product Name", orientation="h", color_discrete_sequence=["#ed7d31"])
        fig.update_layout(height=200, margin=dict(l=150, r=30, t=10, b=30))
        fig.update_traces(texttemplate="%{x}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("<div class="section-header">Category and Product level Performance</div>", unsafe_allow_html=True)
        
        category_perf = filtered.groupby("category_name").agg({
            "order_id": "nunique",
            "quantity": "sum",
            "gross_revenue": "sum",
            "discount_amount": "sum",
            "net_revenue": "sum"
        }).reset_index()
        category_perf.columns = ["Category Name", "Orders", "Quantity", "Gross Revenue", "Discount ($)", "Net Revenue"]
        
        # Add totals
        totals = category_perf.sum(numeric_only=True)
        totals["Category Name"] = "Total"
        category_perf = pd.concat([category_perf, pd.DataFrame([totals])], ignore_index=True)
        
        st.dataframe(category_perf, use_container_width=True, height=400)
    
    # Bottom section
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class="section-header">Unit in Stock and Unit on Order</div>", unsafe_allow_html=True)
        
        stock_data = products_df.groupby("category_name").agg({
            "units_in_stock": "sum",
            "units_on_order": "sum"
        }).reset_index()
        stock_data.columns = ["Category Name", "Units In Stock", "Units On Order"]
        
        totals = stock_data.sum(numeric_only=True)
        totals["Category Name"] = "Total"
        stock_data = pd.concat([stock_data, pd.DataFrame([totals])], ignore_index=True)
        
        st.dataframe(stock_data, use_container_width=True, height=300)
    
    with col2:
        st.markdown("<div class="section-header">Unit in Stock by Category and Product</div>", unsafe_allow_html=True)
        
        category_stock = products_df.groupby("category_name")["units_in_stock"].sum().reset_index()
        fig = px.bar(
            category_stock,
            x="category_name",
            y="units_in_stock",
            color_discrete_sequence=["#2e75b6"]
        )
        fig.update_layout(
            xaxis_title="Category Name",
            yaxis_title="Units In Stock",
            height=300,
            margin=dict(l=50, r=50, t=30, b=100)
        )
        fig.update_traces(texttemplate="%{y}", textposition="outside")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

def employees_page(df, filters):
    """Render Employees page."""
    filtered = apply_filters(df, filters)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class="section-header">Top/Bottom 5 Employees by Orders</div>", unsafe_allow_html=True)
        
        emp_orders = filtered.groupby("employee_name")["order_id"].nunique().reset_index()
        emp_orders.columns = ["Employee Name", "Orders"]
        
        # Top 5
        st.markdown("**Top 5 Employees by order**")
        top5 = emp_orders.nlargest(5, "Orders")
        fig = px.bar(top5, x="Orders", y="Employee Name", orientation="h", color_discrete_sequence=["#70ad47"])
        fig.update_layout(height=200, margin=dict(l=100, r=30, t=10, b=30), yaxis=dict(autorange="reversed"))
        fig.update_traces(texttemplate="%{x}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
        
        # Bottom 5
        st.markdown("**Bottom 5 Employees by orders**")
        bottom5 = emp_orders.nsmallest(5, "Orders")
        fig = px.bar(bottom5, x="Orders", y="Employee Name", orientation="h", color_discrete_sequence=["#ed7d31"])
        fig.update_layout(height=200, margin=dict(l=100, r=30, t=10, b=30))
        fig.update_traces(texttemplate="%{x}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("<div class="section-header">Title and Employee level Performance</div>", unsafe_allow_html=True)
        
        title_perf = filtered.groupby("employee_title").agg({
            "order_id": "nunique",
            "quantity": "sum",
            "gross_revenue": "sum",
            "discount_amount": "sum",
            "net_revenue": "sum"
        }).reset_index()
        title_perf.columns = ["Title", "Orders", "Quantity", "Gross Revenue", "Discount ($)", "Net Revenue"]
        
        totals = title_perf.sum(numeric_only=True)
        totals["Title"] = "Total"
        title_perf = pd.concat([title_perf, pd.DataFrame([totals])], ignore_index=True)
        
        st.dataframe(title_perf, use_container_width=True, height=300)
    
    # Bottom charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class="section-header">Net Revenue by Employee Title</div>", unsafe_allow_html=True)
        
        title_revenue = filtered.groupby("employee_title")["net_revenue"].sum().reset_index()
        title_revenue = title_revenue.sort_values("net_revenue")
        
        fig = go.Figure(go.Waterfall(
            orientation="v",
            x=title_revenue["employee_title"].tolist() + ["Total"],
            y=title_revenue["net_revenue"].tolist() + [title_revenue["net_revenue"].sum()],
            connector={"line": {"color": "rgb(63, 63, 63)"}},
            increasing={"marker": {"color": "#70ad47"}},
            decreasing={"marker": {"color": "#ed7d31"}},
            totals={"marker": {"color": "#2e75b6"}}
        ))
        fig.update_layout(height=300, margin=dict(l=50, r=50, t=30, b=100))
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("<div class="section-header">Net Revenue per order by Employee</div>", unsafe_allow_html=True)
        
        emp_revenue = filtered.groupby("employee_name").agg({
            "order_id": "nunique",
            "net_revenue": "sum"
        }).reset_index()
        emp_revenue["revenue_per_order"] = emp_revenue["net_revenue"] / emp_revenue["order_id"]
        emp_revenue = emp_revenue.sort_values("revenue_per_order", ascending=False)
        
        fig = px.bar(
            emp_revenue,
            x="employee_name",
            y="revenue_per_order",
            color_discrete_sequence=["#2e75b6"]
        )
        fig.update_layout(
            xaxis_title="Employee Name",
            yaxis_title="Net Revenue per order",
            height=300,
            margin=dict(l=50, r=50, t=30, b=100)
        )
        fig.update_traces(texttemplate="%{y:.0f}", textposition="outside")
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

def main():
    # Title
    st.markdown("""
    <div style="background: linear-gradient(90deg, #1f4e79 0%, #2e75b6 100%); 
                padding: 20px; margin-bottom: 20px; border-radius: 5px;">
        <h1 style="color: white; margin: 0;">Northwind Dashboard</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Please run the migration script first: python scripts/migrate_to_snowflake.py")
        return
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Category and Product", "Employees"],
        label_visibility="collapsed"
    )
    
    # Filters
    filters = render_sidebar_filters(df)
    
    # Render selected page
    if page == "Overview":
        overview_page(df, filters)
    elif page == "Category and Product":
        category_product_page(df, filters)
    else:
        employees_page(df, filters)

if __name__ == "__main__":
    main()
