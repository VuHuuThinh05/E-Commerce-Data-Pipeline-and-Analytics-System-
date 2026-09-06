import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy import create_engine

# ============================================================
# PAGE CONFIG & THEME
# ============================================================
st.set_page_config(page_title="E-Commerce Analytics", page_icon="📊", layout="wide")

COLOR_PRIMARY = "#1E3A8A"
COLOR_SECONDARY = "#0D9488"
COLOR_DANGER = "#EF4444"

# ============================================================
# CUSTOM CSS FOR HIGH-END UI/UX
# ============================================================
# ============================================================
# CUSTOM CSS FOR HIGH-END UI/UX
# ============================================================
st.markdown("""
    <style>
    /* Chèn bản đồ Brazil chìm làm Background */
    .main {
        padding: 0.5rem 1rem;
        background-image: linear-gradient(rgba(255, 255, 255, 0.92), rgba(255, 255, 255, 0.92)), 
                          url('https://cdn.diaocthongthai.com/map/BRA/map_hd_0/atlas-political-map-full.png');
        background-repeat: no-repeat;
        background-position: center 120px;
        background-size: 650px;
        background-attachment: fixed;
    }

    /* Giữ nguyên CSS cũ của Metric Card */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #F8FAFC 0%, #EFF6FF 100%);
        padding: 16px;
        border-radius: 12px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    div[data-testid="stMetric"] label { color: #64748B !important; font-weight: 600 !important; font-size: 0.85rem !important; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: #1E293B !important; font-weight: 700 !important; font-size: 1.5rem !important; }
    hr { margin: 1rem 0; border-color: #E2E8F0; }
    </style>
""", unsafe_allow_html=True)

# ============================================================
# CONNECT TO POSTGRESQL
# ============================================================
@st.cache_resource
def get_connection():
    return create_engine("postgresql://airflow:airflow@localhost:5432/ecommerce")

@st.cache_data(ttl=60)
def load_data(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

# ============================================================
# HEADER
# ============================================================
st.markdown("""
    <div style="text-align: center; padding: 10px 0 20px 0;">
        <h1 style="color: #1E3A8A; font-size: 2.6rem; font-weight: 800; margin-bottom: 8px;">
            📊 E-Commerce Analytics Dashboard
        </h1>
        <p style="color: #475569; font-size: 1.15rem; font-weight: 500;">
            🇧🇷 Brazilian E-Commerce Data Pipeline — Business Intelligence Overview
        </p>
    </div>
""", unsafe_allow_html=True)
st.markdown("---")
# ============================================================
# REFRESH BUTTON & QUICK METRICS BAR
# ============================================================
col_hdr1, col_hdr2 = st.columns([8, 2])
with col_hdr2:
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ============================================================
# ENHANCED SIDEBAR FILTERS
# ============================================================
st.sidebar.header("🔎 Advanced Filters")

# Khai báo region_map ở phạm vi toàn cục
region_map = {
    "Southeast (Đông Nam)": ["SP", "RJ", "MG", "ES"],
    "South (Miền Nam)": ["PR", "RS", "SC"],
    "Northeast (Đông Bắc)": ["BA", "PE", "CE", "MA", "PB", "PI", "RN", "AL", "SE"],
    "Center-West (Trung Tây)": ["DF", "GO", "MT", "MS"],
    "North (Miền Bắc)": ["AM", "PA", "RO", "TO", "AC", "AP", "RR"]
}

filter_data = load_data("""
    SELECT
        f.order_date,
        f.order_status,
        c.customer_state,
        f.total_price
    FROM analytics.fact_orders f
    LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
    WHERE f.order_date IS NOT NULL
""")

if not filter_data.empty:
    filter_data["order_date"] = pd.to_datetime(filter_data["order_date"])
    min_date = filter_data["order_date"].min().date()
    max_date = filter_data["order_date"].max().date()

    # 1. Date Range
    date_range = st.sidebar.date_input("Order Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    # 2. Region Group Filter
    selected_region = st.sidebar.selectbox("Region Group", ["All Regions"] + list(region_map.keys()))

    # 3. Customer State (Filtering by Region if selected)
    states = sorted(filter_data["customer_state"].dropna().unique().tolist())
    if selected_region != "All Regions":
        states = [s for s in states if s in region_map[selected_region]]
    selected_states = st.sidebar.multiselect("Customer State", states, default=[])

    # 4. Order Status
    statuses = sorted(filter_data["order_status"].dropna().unique().tolist())
    selected_statuses = st.sidebar.multiselect("Order Status", statuses, default=[])

    # 5. Order Value Range Slider
    max_val_limit = float(filter_data["total_price"].max())
    price_range = st.sidebar.slider("Order Value Range (R$)", 0.0, float(min(max_val_limit, 2000.0)), (0.0, float(min(max_val_limit, 2000.0))))

    # 6. Minimum Review Score
    min_review = st.sidebar.selectbox("Minimum Review Score", [1, 2, 3, 4, 5], index=0)
else:
    date_range = None
    selected_region = "All Regions"
    selected_states = []
    selected_statuses = []
    price_range = (0.0, 2000.0)
    min_review = 1

# ============================================================
# BUILD FILTER CONDITION
# ============================================================
conditions = []

if date_range and len(date_range) == 2:
    conditions.append(f"f.order_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")

if selected_states:
    states_sql = ", ".join(f"'{s}'" for s in selected_states)
    conditions.append(f"c.customer_state IN ({states_sql})")
elif selected_region != "All Regions":
    reg_states = ", ".join(f"'{s}'" for s in region_map[selected_region])
    conditions.append(f"c.customer_state IN ({reg_states})")

if selected_statuses:
    statuses_sql = ", ".join(f"'{s}'" for s in selected_statuses)
    conditions.append(f"f.order_status IN ({statuses_sql})")

if price_range:
    conditions.append(f"COALESCE(f.total_price, 0) BETWEEN {price_range[0]} AND {price_range[1]}")

filter_condition = " AND ".join(conditions) if conditions else "1=1"

# Subquery filter cho Review Score
review_subquery = f"f.order_id IN (SELECT order_id FROM analytics.fact_reviews WHERE review_score >= {min_review})"
full_condition = f"{filter_condition} AND {review_subquery}"

# ============================================================
# KPI METRICS WITH BUSINESS INSIGHTS
# ============================================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_orders = load_data(f"""
        SELECT COUNT(DISTINCT f.order_id) AS cnt
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
        WHERE {full_condition}
    """)
    st.metric(label="Total Orders", value=f"{int(total_orders['cnt'].iloc[0]):,}", delta="Volume")

with col2:
    total_revenue = load_data(f"""
        SELECT COALESCE(SUM(f.total_price), 0) AS rev
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
        WHERE {full_condition}
    """)
    st.metric(label="Total Revenue", value=f"R$ {total_revenue['rev'].iloc[0]:,.2f}", delta="Gross Revenue")

with col3:
    avg_order = load_data(f"""
        SELECT COALESCE(AVG(f.total_price), 0) AS avg_value
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
        WHERE {full_condition}
    """)
    st.metric(label="Avg Order Value (AOV)", value=f"R$ {avg_order['avg_value'].iloc[0]:,.2f}", delta="Per Order")

with col4:
    total_customers = load_data(f"""
        SELECT COUNT(DISTINCT f.customer_unique_id) AS cnt
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
        WHERE {full_condition}
    """)
    st.metric(label="Total Customers", value=f"{int(total_customers['cnt'].iloc[0]):,}", delta="Unique Buyers")

st.markdown("---")

# ============================================================
# ENTERPRISE DASHBOARD TABS
# ============================================================
# Thay bằng dòng code mới này:
tab_overview, tab_logistics, tab_forecasting = st.tabs([
    "📊 Commercial & Sales Overview", 
    "🚚 Logistics & Operations", 
    "🔮 What-If Scenario & Forecasting"
])

# ------------------------------------------------------------
# TAB 1: COMMERCIAL & SALES OVERVIEW
# ------------------------------------------------------------
with tab_overview:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Daily Revenue Trend")
        revenue_data = load_data(f"""
            SELECT f.order_date::date AS date, SUM(f.total_price) AS revenue, COUNT(DISTINCT f.order_id) AS orders
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition}
            GROUP BY f.order_date::date ORDER BY f.order_date::date
        """)
        if not revenue_data.empty:
            fig = px.area(revenue_data, x="date", y="revenue", title="Revenue Performance Over Time", labels={"revenue": "Revenue (R$)", "date": "Date"}, color_discrete_sequence=[COLOR_PRIMARY])
            fig.update_traces(line_width=2, fillcolor="rgba(30, 58, 138, 0.1)")
            fig.update_layout(hovermode="x unified", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data available.")

    with col2:
        st.subheader("🗺️ Revenue by State")
        state_data = load_data(f"""
            SELECT c.customer_state, SUM(f.total_price) AS revenue, COUNT(DISTINCT f.order_id) AS orders
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition}
            GROUP BY c.customer_state ORDER BY revenue DESC LIMIT 10
        """)
        if not state_data.empty:
            fig = px.bar(state_data, x="customer_state", y="revenue", title="Top 10 States by Revenue", labels={"revenue": "Revenue (R$)", "customer_state": "State"}, color="revenue", color_continuous_scale="Viridis")
            fig.update_layout(showlegend=False, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No state data available.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📦 Orders by State (Top 10)")
        orders_by_state = load_data(f"""
            SELECT c.customer_state, COUNT(DISTINCT f.order_id) AS total_orders
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition}
            GROUP BY c.customer_state ORDER BY total_orders DESC LIMIT 10
        """)
        if not orders_by_state.empty:
            fig = px.bar(orders_by_state, x="total_orders", y="customer_state", orientation="h", title="Order Share by Top States", labels={"total_orders": "Total Orders", "customer_state": "State"}, color="total_orders", color_continuous_scale="Blues")
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No order data available.")

    with col2:
        st.subheader("💰 Average Order Value by State")
        avg_by_state = load_data(f"""
            SELECT c.customer_state, AVG(f.total_price) AS avg_value
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition}
            GROUP BY c.customer_state ORDER BY avg_value DESC LIMIT 10
        """)
        if not avg_by_state.empty:
            fig = px.bar(avg_by_state, x="avg_value", y="customer_state", orientation="h", title="Top AOV by Regional Customers", labels={"avg_value": "Avg Order Value (R$)", "customer_state": "State"}, color="avg_value", color_continuous_scale="Teal")
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No average order value data available.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🛍️ Revenue by Product Category")
        category_data = load_data(f"""
            SELECT COALESCE(p.product_category_name_english, p.product_category_name, 'Unknown') AS category, SUM(oi.price) AS revenue
            FROM analytics.fact_order_items oi
            JOIN analytics.fact_orders f ON oi.order_id = f.order_id
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            LEFT JOIN analytics.dim_products p ON oi.product_key = p.product_key
            WHERE {full_condition}
            GROUP BY category ORDER BY revenue DESC LIMIT 10
        """)
        if not category_data.empty:
            fig = px.bar(category_data, x="revenue", y="category", orientation="h", title="Top 10 Product Categories by Revenue", labels={"revenue": "Revenue (R$)", "category": "Category"}, color="revenue", color_continuous_scale="Cividis")
            fig.update_layout(yaxis={"categoryorder": "total ascending"}, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No product category data available.")

    with col2:
        st.subheader("💳 Payment Method Breakdown")
        payment_data = load_data(f"""
            SELECT payment_type, SUM(payment_value) AS payment_value, COUNT(DISTINCT order_id) AS orders
            FROM analytics.fact_payments p
            WHERE p.order_id IN (
                SELECT f.order_id FROM analytics.fact_orders f
                LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
                WHERE {full_condition}
            )
            GROUP BY payment_type ORDER BY payment_value DESC
        """)
        if not payment_data.empty:
            fig = px.pie(payment_data, values="payment_value", names="payment_type", title="Payment Method Share by Value", hole=0.45, color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No payment data available.")

    st.subheader("⭐ Customer Review Score Breakdown")
    review_data = load_data(f"""
        SELECT r.review_score, COUNT(*) AS reviews
        FROM analytics.fact_reviews r
        WHERE r.order_id IN (
            SELECT f.order_id FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition}
        )
        GROUP BY r.review_score ORDER BY r.review_score
    """)
    if not review_data.empty:
        # Sửa px.colors.qualitative.Spectral thành px.colors.diverging.Spectral
        fig = px.bar(review_data, x="review_score", y="reviews", title="Review Score Distribution (1-5 Stars)", labels={"review_score": "Review Score", "reviews": "Number of Reviews"}, color="review_score", color_discrete_sequence=px.colors.diverging.Spectral)
        fig.update_layout(xaxis=dict(dtick=1), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No review data available.")

# ------------------------------------------------------------
# TAB 2: LOGISTICS & OPERATIONS
# ------------------------------------------------------------
with tab_logistics:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🚚 Average Delivery Duration (Days)")
        delivery_data = load_data(f"""
            SELECT f.order_date::date AS date, AVG(f.delivery_days) AS avg_delivery_days
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition} AND f.delivery_days IS NOT NULL
            GROUP BY f.order_date::date ORDER BY f.order_date::date
        """)
        if not delivery_data.empty:
            fig = px.line(delivery_data, x="date", y="avg_delivery_days", title="Average Delivery Duration Over Time", labels={"date": "Date", "avg_delivery_days": "Delivery Days"}, color_discrete_sequence=[COLOR_SECONDARY])
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No delivery data available.")

    with col2:
        st.subheader("⏱️ SLA Delivery Delay Analysis")
        delay_data = load_data(f"""
            SELECT
                CASE
                    WHEN f.delivery_delay_days <= 0 THEN 'On Time / Early'
                    WHEN f.delivery_delay_days <= 3 THEN '1-3 Days Late'
                    WHEN f.delivery_delay_days <= 7 THEN '4-7 Days Late'
                    ELSE 'More than 7 Days Late'
                END AS delay_group,
                COUNT(*) AS orders
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
            WHERE {full_condition} AND f.delivery_delay_days IS NOT NULL
            GROUP BY delay_group ORDER BY orders DESC
        """)
        if not delay_data.empty:
            fig = px.bar(delay_data, x="delay_group", y="orders", title="Orders by Delivery SLA Delay", labels={"delay_group": "SLA Category", "orders": "Orders"}, color="delay_group", color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No delay data available.")

    st.subheader("📊 Order Status Overview")
    status_data = load_data(f"""
        SELECT f.order_status, COUNT(DISTINCT f.order_id) AS orders
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
        WHERE {full_condition}
        GROUP BY f.order_status ORDER BY orders DESC
    """)
    if not status_data.empty:
        fig = px.bar(status_data, x="order_status", y="orders", title="Order Fulfillment Status", labels={"order_status": "Status", "orders": "Orders"}, color="order_status", color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# ============================================================
# CẬP NHẬT TAB MỚI: TÍCH HỢP VÀO ST.TABS
# ============================================================
# Tìm dòng st.tabs cũ và sửa thành 3 tabs:
# tab_overview, tab_logistics, tab_forecasting = st.tabs([
#     "📊 Commercial & Sales Overview", 
#     "🚚 Logistics & Operations", 
#     "🔮 What-If Scenario & Forecasting"
# ])

# ------------------------------------------------------------
# TAB 3: EXECUTIVE FORECASTING & WHAT-IF SIMULATION
# ------------------------------------------------------------
with tab_forecasting:
    st.subheader("🎲 What-If Scenario Simulation (Mô phỏng Kịch bản Kinh doanh)")
    st.caption("Điều chỉnh các tham số giả định bên dưới để dự báo tác động doanh thu trong cuộc họp chiến lược:")
    
    col_sim1, col_sim2, col_sim3 = st.columns(3)
    with col_sim1:
        growth_rate = st.slider("Dự báo % Tăng trưởng Đơn hàng", -30, 50, 10, step=5) / 100.0
    with col_sim2:
        aov_change = st.slider("Dự báo % Thay đổi Giá trị Đơn (AOV)", -20, 30, 5, step=5) / 100.0
    with col_sim3:
        freight_impact = st.slider("Biến động Chi phí Vận chuyển (R$)", -5.0, 15.0, 0.0, step=1.0)

    # Tính toán con số Base từ Database
    base_rev_val = total_revenue["rev"].iloc[0] if not total_revenue.empty else 0
    base_orders_val = total_orders["cnt"].iloc[0] if not total_orders.empty else 0
    base_aov_val = avg_order["avg_value"].iloc[0] if not avg_order.empty else 0

    # Tính toán Scenario
    sim_orders = base_orders_val * (1 + growth_rate)
    sim_aov = base_aov_val * (1 + aov_change)
    sim_revenue = sim_orders * sim_aov

    st.markdown("#### 📊 Bảng So sánh Kịch bản Giả định vs Thực tế")
    col_res1, col_res2, col_res3 = st.columns(3)
    
    with col_res1:
        st.metric(
            label="Dự báo Tổng Doanh thu", 
            value=f"R$ {sim_revenue:,.2f}", 
            delta=f"{(sim_revenue - base_rev_val):,.2f} R$ ({((sim_revenue/base_rev_val)-1)*100:.1f}%)" if base_rev_val > 0 else "N/A"
        )
    with col_res2:
        st.metric(
            label="Dự báo Số lượng Đơn hàng", 
            value=f"{int(sim_orders):,}", 
            delta=f"{int(sim_orders - base_orders_val):,} đơn"
        )
    with col_res3:
        st.metric(
            label="Dự báo Giá trị Đơn trung bình (AOV)", 
            value=f"R$ {sim_aov:,.2f}", 
            delta=f"{(sim_aov - base_aov_val):,.2f} R$"
        )

    st.markdown("---")
    st.subheader("📈 Trend Line Projection (Dự báo Đường xu hướng)")
    
    # Dự báo xu hướng đơn giản bằng Moving Average 30 ngày
    ts_data = load_data(f"""
        SELECT f.order_date::date AS date, SUM(f.total_price) AS revenue
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
        WHERE {full_condition}
        GROUP BY f.order_date::date ORDER BY f.order_date::date
    """)

    if not ts_data.empty:
        ts_data["date"] = pd.to_datetime(ts_data["date"])
        ts_data["MA_7"] = ts_data["revenue"].rolling(window=7).mean()
        ts_data["MA_30"] = ts_data["revenue"].rolling(window=30).mean()

        fig_fc = px.line(
            ts_data, x="date", y=["revenue", "MA_7", "MA_30"],
            labels={"value": "Revenue (R$)", "date": "Date", "variable": "Chỉ số"},
            title="Đường xu hướng Doanh thu Thực tế & Trung bình động (MA7 vs MA30)",
            color_discrete_map={"revenue": "#94A3B8", "MA_7": COLOR_SECONDARY, "MA_30": COLOR_PRIMARY}
        )
        fig_fc.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_fc, use_container_width=True)

# ============================================================
# RECENT DELIVERED ORDERS TABLE
# ============================================================
st.subheader("📋 Recent Delivered Transactions")

col1, col2 = st.columns([3, 1])
with col2:
    limit = st.selectbox("Show records:", [10, 20, 50, 100], index=1)

recent_orders = load_data(f"""
    SELECT
        LEFT(f.order_id, 8) || '...' AS order_id_short,
        c.customer_city,
        c.customer_state,
        f.total_price,
        f.total_freight,
        COALESCE(f.num_products, 1) AS num_products,
        f.order_status,
        f.order_date
    FROM analytics.fact_orders f
    LEFT JOIN analytics.dim_customers c ON f.customer_unique_id = c.customer_unique_id
    WHERE {full_condition} AND f.order_status = 'delivered' AND f.total_price > 0
    ORDER BY f.order_date DESC
    LIMIT {limit}
""")

if not recent_orders.empty:
    recent_orders["order_date"] = pd.to_datetime(recent_orders["order_date"]).dt.strftime("%Y-%m-%d")
    st.dataframe(
        recent_orders,
        use_container_width=True,
        hide_index=True,
        column_config={
            "order_id_short": st.column_config.TextColumn("Order ID", width="small"),
            "customer_city": "City",
            "customer_state": "State",
            "total_price": st.column_config.NumberColumn("Total Price", format="R$ %.2f"),
            "total_freight": st.column_config.NumberColumn("Freight", format="R$ %.2f"),
            "num_products": st.column_config.NumberColumn("Products", format="%d"),
            "order_status": "Status",
            "order_date": "Order Date"
        }
    )
else:
    st.info("No delivered orders found matching criteria.")

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.markdown("**Data Pipeline:** Kafka Stream → PostgreSQL RAW → dbt Staging → Analytics Marts → Streamlit Enterprise Dashboard")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")