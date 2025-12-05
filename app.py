import streamlit as st
from datetime import datetime, timedelta
from src.ui.tabs.kpi import render_kpi_tab
from src.ui.tabs.details import render_details_tab
from src.ui.tabs.ads import render_ads_tab
from src.ui.tabs.quality import render_quality_tab
from src.ui.tabs.automation import render_automation_tab
from src.ui.tabs.correlation import render_correlation_tab
from src.services.etl import etl_service

st.set_page_config(page_title="Unified Marketing Dashboard", layout="wide")

def main():
    st.title("🚀 Unified Marketing Dashboard (v2.0)")
    
    # Sidebar
    st.sidebar.title("Settings")
    
    # Date Selection
    today = datetime.now()
    default_start = today - timedelta(days=30)
    
    start_date = st.sidebar.date_input("Start Date", default_start)
    end_date = st.sidebar.date_input("End Date", today)
    
    if start_date > end_date:
        st.sidebar.error("Start date must be before end date.")
        return

    st.sidebar.divider()
    
    # ETL Controls
    st.sidebar.subheader("Data Sync")
    if st.sidebar.button("Run ETL (Sync Data)"):
        with st.spinner("Syncing data... This may take a while."):
            try:
                # Initialize DB first just in case
                etl_service.run_init()
                # Run ETL
                etl_service.run_etl(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
                st.sidebar.success("Data synced successfully!")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"ETL Failed: {e}")

    # Tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "KPI Dashboard", 
        "Details", 
        "Ads Performance", 
        "Data Quality", 
        "Ad Automation",
        "O2O相関分析"
    ])
    
    with tab1:
        render_kpi_tab(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    
    with tab2:
        render_details_tab(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        
    with tab3:
        render_ads_tab(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        
    with tab4:
        render_quality_tab()

    with tab5:
        render_automation_tab()
        
    with tab6:
        render_correlation_tab()

if __name__ == "__main__":
    main()
