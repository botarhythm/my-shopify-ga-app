import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.database.db import db

from src.ui.components.glossary import get_term_label, get_term_help

def render_kpi_tab(start_date, end_date):
    st.header("📊 KPI Dashboard")
    
    # Glossary Expander for Beginners
    with st.expander("🔰 初心者向けガイド: 各指標の見方"):
        st.markdown("""
        - **売上総額**: 期間内の合計売上です（全チャネル含む）。
        - **広告経由売上**: Google広告をクリックして購入に至った売上のみです。
        - **広告費**: Google広告などで使った費用です。
        - **ROAS**: 広告の「燃費」です。**広告経由売上 ÷ 広告費**で計算します。1.0以上なら元は取れていますが、利益を出すには2.0~3.0以上を目指しましょう。
        - **セッション数**: お店（サイト）に来てくれたお客さんの延べ人数のようなものです。
        
        ⚠️ **重要**: ROASは「広告経由売上」のみを使って計算しています。全売上を使うと、広告の効果が実際より高く見えてしまうためです。
        """)
    
    con = db.get_connection(read_only=True)
    try:
        # Fetch Marketing Performance Data + Sales Breakdown
        # Join marketing_performance with daily_revenue to get breakdown
        query = """
            SELECT 
                m.date,
                m.ad_cost,
                m.ad_attributed_sales,
                m.sessions,
                m.total_sales,
                m.roas,
                d.shopify_sales,
                d.square_sales
            FROM marts.marketing_performance m
            LEFT JOIN marts.daily_revenue d ON m.date = d.date
            WHERE m.date BETWEEN ? AND ?
            ORDER BY m.date
        """
        df = con.execute(query, [start_date, end_date]).df()
        
        if df.empty:
            st.warning("No data available for the selected period.")
            return

        # Fill NaN with 0 for visualization
        df = df.fillna(0)

        # KPI Cards
        total_sales = df['total_sales'].sum()
        shopify_total = df['shopify_sales'].sum()
        square_total = df['square_sales'].sum()
        
        ad_attributed_sales = df['ad_attributed_sales'].sum()
        total_spend = df['ad_cost'].sum()
        total_roas = ad_attributed_sales / total_spend if total_spend > 0 else 0
        total_sessions = df['sessions'].sum()
        
        # Row 1: High Level
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric(
            label=get_term_label("Total Sales"), 
            value=f"¥{total_sales:,.0f}",
            help=get_term_help("Total Sales")
        )
        col2.metric(
            label="Shopify (Online)", 
            value=f"¥{shopify_total:,.0f}",
            help="オンラインストアの売上"
        )
        col3.metric(
            label="Square (Store)", 
            value=f"¥{square_total:,.0f}",
            help="実店舗の売上"
        )
        col4.metric(
            label=get_term_label("Ad Attributed Sales"), 
            value=f"¥{ad_attributed_sales:,.0f}",
            help=get_term_help("Ad Attributed Sales")
        )
        col5.metric(
            label=get_term_label("ROAS"), 
            value=f"{total_roas:.2f}",
            help=get_term_help("ROAS")
        )
        
        st.divider()
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Sales Trend (Breakdown)")
            # Stacked Bar Chart for Sales Breakdown
            fig_sales = go.Figure()
            fig_sales.add_trace(go.Bar(
                x=df['date'], 
                y=df['shopify_sales'], 
                name='Shopify (Online)',
                marker_color='#95BF47' # Shopify Green-ish
            ))
            fig_sales.add_trace(go.Bar(
                x=df['date'], 
                y=df['square_sales'], 
                name='Square (Store)',
                marker_color='#3E4348' # Square Grey-ish
            ))
            fig_sales.update_layout(barmode='stack', title="Daily Sales by Channel")
            st.plotly_chart(fig_sales, use_container_width=True)
            
        with col2:
            st.subheader("ROAS Trend")
            fig_roas = px.line(df, x='date', y='roas', title="Daily ROAS")
            # Add reference line for ROAS = 1.0
            fig_roas.add_hline(y=1.0, line_dash="dash", line_color="red")
            st.plotly_chart(fig_roas, use_container_width=True)
            
        # Detailed Data
        with st.expander("View Detailed Data"):
            st.dataframe(df, use_container_width=True)
            
    finally:
        con.close()
