import streamlit as st
from src.database.db import db

def render_quality_tab():
    st.header("✅ Data Quality")
    
    if st.button("Run Quality Checks"):
        con = db.get_connection(read_only=True)
        try:
            # Check 1: Recent Data
            st.subheader("1. Data Freshness")
            tables = ['core.shopify_orders', 'core.square_payments', 'core.ga4_daily', 'core.ads_campaign']
            for table in tables:
                try:
                    res = con.execute(f"SELECT MAX(date) FROM {table}").fetchone()
                    st.write(f"**{table}**: Last date = {res[0]}")
                except Exception as e:
                    st.error(f"Error checking {table}: {e}")

            # Check 2: Record Counts
            st.subheader("2. Record Counts")
            for table in tables:
                try:
                    res = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    st.write(f"**{table}**: {res[0]} rows")
                except:
                    pass

        finally:
            con.close()
