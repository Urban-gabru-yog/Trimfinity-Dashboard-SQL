import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_extras.colored_header import colored_header
import mysql.connector
import os
import datetime
from dotenv import load_dotenv

st.set_page_config(page_title="Trimfinity Voice Agent Dashboard", layout="wide", page_icon="📞")
load_dotenv()

# --- DATABASE CONNECTION ---
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

# --- LOAD DATA ---
def load_data():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM merged_data", conn)
        calls = pd.read_sql("SELECT * FROM calls", conn)
    except Exception as e:
        st.error(f"❌ DB Query Failed: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()
    return df, calls

df, calls = load_data()
if df.empty:
    st.warning("⚠️ No merged data found. Please run fetch and merge scripts first.")
    st.stop()

# --- PREPROCESSING ---
df['StartTimestamp'] = pd.to_datetime(df['StartTimestamp'], errors='coerce')
df['created_at'] = pd.to_datetime(df.get('created_at'), errors='coerce')
df['call_date'] = df['StartTimestamp'].dt.date
df['TotalDurationSec'] = pd.to_numeric(df['TotalDurationSec'], errors='coerce').fillna(0)
df['TotalCost'] = pd.to_numeric(df['TotalCost'], errors='coerce').fillna(0)
df['total_price'] = pd.to_numeric(df['total_price'], errors='coerce').fillna(0)
df['COGS'] = pd.to_numeric(df['COGS'], errors='coerce').fillna(0)

calls['StartTimestamp'] = pd.to_datetime(calls['StartTimestamp'], errors='coerce')
calls['call_date'] = calls['StartTimestamp'].dt.date
if 'TotalCost' in calls.columns:
    calls['TotalCost'] = pd.to_numeric(calls['TotalCost'], errors='coerce').fillna(0)
else:
    calls['TotalCost'] = pd.Series(0, index=calls.index)

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.image("logo.png", width=150)
    st.markdown("## 🔍 Filter by Date Range")
    valid_dates = df['call_date'].dropna()
    call_date_min = valid_dates.min() if not valid_dates.empty else datetime.date.today()
    call_date_max = valid_dates.max() if not valid_dates.empty else datetime.date.today()
    start_date = st.date_input("Start Date", call_date_min)
    end_date = st.date_input("End Date", call_date_max)
    granularity = st.selectbox("Group By", ["Day", "Week", "Month", "Quarter"])

# --- FILTERED DATA ---
df_filtered = df[(df['call_date'] >= start_date) & (df['call_date'] <= end_date)].copy()
calls_filtered = calls[(calls['call_date'] >= start_date) & (calls['call_date'] <= end_date)].copy()

# --- METRICS CALC ---
total_calls = len(calls_filtered)
connected_calls = len(calls_filtered[calls_filtered['TotalDurationSec'] > 1])
total_call_cost = calls_filtered['TotalCost'].sum()
total_call_duration = calls_filtered['TotalDurationSec'].sum()
total_call_hms = str(datetime.timedelta(seconds=int(total_call_duration)))

# --- CUSTOMER CONVERSION ---
purchase_df = df_filtered[
    df_filtered['order_number'].notna() &
    df_filtered['title'].notna() &
    df_filtered['created_at'].notna() &
    (df_filtered['StartTimestamp'] <= df_filtered['created_at'])
].copy()

purchase_df = purchase_df.drop_duplicates(subset='Email', keep='first')
purchase_df['Call Time'] = purchase_df['StartTimestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
purchase_df['Order Time'] = purchase_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

purchase_df.rename(columns={
    "call_date": "Date",
    "Email": "Customer Email",
    "order_number": "Order Number",
    "title": "Product Purchased",
    "total_price": "Price"
}, inplace=True)

table = purchase_df[[
    "Date", "Customer Email", "Order Number", "Call Time",
    "Order Time", "Product Purchased", "Price", "COGS"
]]
table['COGS'] = pd.to_numeric(table['COGS'], errors='coerce').fillna(0)

# KPI calculations
total_purchases = table['Customer Email'].nunique()
total_revenue = table['Price'].sum()
total_cogs = table['COGS'].sum()
conversion = round((total_purchases / connected_calls) * 100, 2) if connected_calls > 0 else 0
profit = ((total_revenue / 118) * 100) - total_cogs - total_call_cost - (120 * total_purchases)

# --- DASHBOARD HEADER + METRICS ---
st.title("Trimfinity Voice Agent Dashboard")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📞 Total Calls", total_calls)
col2.metric("✅ Connected Calls", connected_calls)
col3.metric("⏱️ Total Duration", total_call_hms)
col4.metric("💰 Total Call Cost", f"₹{total_call_cost:,.2f}")
col5.metric("📦 Total COGS Price", f"₹{total_cogs:,.2f}")

col6, col7, col8, col9, col10 = st.columns(5)
col6.metric("🍭 Purchases", total_purchases)
col7.metric("🔁 Conversion Rate", f"{conversion}%")
col8.metric("💵 Revenue", f"₹{total_revenue:,.2f}")
col9.metric("💸 Profit", f"₹{profit:,.2f}")

# --- MONTHLY REVENUE & PROFIT GRAPH ---
colored_header("📊 Revenue & Profit Trend", "", color_name="blue-70")
if not table.empty:
    temp_df = table.copy()
    temp_df['created_at'] = pd.to_datetime(df_filtered['created_at'], errors='coerce')
    if granularity == "Week":
        temp_df['Period'] = temp_df['created_at'].dt.to_period('W').dt.start_time
    elif granularity == "Month":
        temp_df['Period'] = temp_df['created_at'].dt.to_period('M').dt.start_time
    elif granularity == "Quarter":
        temp_df['Period'] = temp_df['created_at'].dt.to_period('Q').dt.start_time
    else:
        temp_df['Period'] = temp_df['created_at'].dt.date

    rev_profit_df = temp_df.groupby('Period').agg({
        'Price': 'sum',
        'COGS': 'sum',
        'Customer Email': 'nunique'
    }).reset_index()

    rev_profit_df['Profit'] = ((rev_profit_df['Price'] / 118) * 100) - rev_profit_df['COGS'] - (total_call_cost * rev_profit_df['Customer Email'] / total_purchases) - (120 * rev_profit_df['Customer Email'])

    # Rename columns for clarity in legend
    rev_profit_df.rename(columns={"Price": "Revenue", "Profit": "Profit ₹"}, inplace=True)

    fig_rev = px.line(rev_profit_df, x="Period", y=["Revenue", "Profit ₹"], markers=True, title=f"Revenue & Profit by {granularity}")
    fig_rev.update_layout(xaxis_title=granularity, yaxis_title="Amount (₹)")
    # Set profit line to green
    fig_rev.update_traces(selector=dict(name="Profit ₹"), line=dict(color="green"))
    st.plotly_chart(fig_rev, use_container_width=True)

# --- CALL DURATION HISTOGRAM ---
colored_header("📊 Call Duration Distribution", "", color_name="blue-70")
fig_hist = px.histogram(df_filtered, x="TotalDurationSec", nbins=30, title="Call Duration (Seconds)")
st.plotly_chart(fig_hist, use_container_width=True)

# --- CUSTOMER TABLE ---
colored_header("🍭 Customers Who Made a Purchase", "", color_name="gray-70")
st.dataframe(table, use_container_width=True)

# --- PRODUCT DISTRIBUTION ---
if not table.empty:
    colored_header("🌟 Product Purchase Distribution", "", color_name="green-70")
    pie_df = table['Product Purchased'].value_counts().reset_index()
    pie_df.columns = ['Product', 'Count']
    fig_pie = px.pie(pie_df, names='Product', values='Count', hole=0.4)
    st.plotly_chart(fig_pie, use_container_width=True)

# --- COUPON TRACKER ---
colored_header("🏽 OFF5 Coupon Usage", "", color_name="red-70")

def extract_coupon(discounts):
    try:
        codes = eval(discounts) if isinstance(discounts, str) else discounts
        if isinstance(codes, list):
            for d in codes:
                if d.get("code", "").upper() == "OFF5":
                    return "OFF5"
    except:
        return None
    return None

df_filtered["Coupon Code"] = df_filtered["discount_codes"].apply(extract_coupon)
off5_df = df_filtered[df_filtered["Coupon Code"] == "OFF5"]

if not off5_df.empty:
    coupon_df = off5_df[["customer_first_name", "Email", "order_number", "Coupon Code"]].drop_duplicates()
    coupon_df.columns = ["Customer Name", "Customer Email", "Order Number", "Coupon Code"]
    st.dataframe(coupon_df, use_container_width=True)
    st.download_button("⬇️ Download OFF5 Users", coupon_df.to_csv(index=False), "off5_users.csv", "text/csv")
else:
    st.info("No OFF5 coupon usage found for selected range.")
