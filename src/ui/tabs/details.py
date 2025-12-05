import streamlit as st
import plotly.express as px
from src.database.db import db

def render_details_tab(start_date, end_date):
    st.header("🔍 Details Analysis")
    
    with st.expander("🔰 初心者向けガイド: 売上の内訳について"):
        st.markdown("""
        - **Shopify**: オンラインストア（ECサイト）での売上です。
        - **Square**: 実店舗やイベントなど、対面販売（POSレジ）での売上です。
        """)
    
    con = db.get_connection(read_only=True)
    try:
        # Sales Breakdown
        st.subheader("Sales Breakdown (Shopify vs Square)")
        query_breakdown = """
            SELECT 
                SUM(shopify_sales) as shopify,
                SUM(square_sales) as square
            FROM marts.daily_revenue
            WHERE date BETWEEN ? AND ?
        """
        df_breakdown = con.execute(query_breakdown, [start_date, end_date]).df()
        
        if not df_breakdown.empty:
            shopify_val = df_breakdown['shopify'].iloc[0] or 0
            square_val = df_breakdown['square'].iloc[0] or 0
            
            fig_pie = px.pie(
                values=[shopify_val, square_val],
                names=['Shopify', 'Square'],
                title="Revenue Share"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
        # Top Products (Shopify + Square Combined)
        st.subheader("Top Products (Shopify + Square)")
        
        with st.expander("ℹ️ この表について"):
            st.markdown("""
            - **Total Revenue**: オンライン（Shopify）と店舗（Square）を合わせた売上合計
            - **Shopify Revenue**: オンラインストアでの売上
            - **Square Revenue**: 実店舗での売上
            """)
        
        query_products = """
            SELECT 
                product_name,
                total_quantity,
                total_revenue,
                shopify_revenue,
                square_revenue
            FROM marts.product_sales
            ORDER BY total_revenue DESC
        """
        df_products = con.execute(query_products).df()
        
        if not df_products.empty:
            # Calculate Share
            total_sales_sum = df_products['total_revenue'].sum()
            df_products['share'] = (df_products['total_revenue'] / total_sales_sum) * 100

            # Slice for Chart (Top 10)
            df_chart = df_products.head(10).sort_values(by='total_revenue', ascending=True) # Sort for chart

            # Create stacked bar chart
            import plotly.graph_objects as go
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Shopify',
                y=df_chart['product_name'],
                x=df_chart['shopify_revenue'],
                orientation='h',
                marker_color='#95BF47'  # Shopify green
            ))
            
            fig.add_trace(go.Bar(
                name='Square',
                y=df_chart['product_name'],
                x=df_chart['square_revenue'],
                orientation='h',
                marker_color='#3E4348'  # Square dark
            ))
            
            fig.update_layout(
                title="Top 10 Products by Revenue (Shopify + Square)",
                barmode='stack',
                xaxis_title="Revenue",
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show data table
            with st.expander("📋 詳細データを見る", expanded=True):
                display_df = df_products.copy()
                display_df = display_df[['product_name', 'total_revenue', 'share', 'total_quantity', 'shopify_revenue', 'square_revenue']]
                display_df.columns = ['商品名', '合計売上', '売上構成比', '販売数', 'Shopify売上', 'Square売上']
                
                # 1-based index
                display_df.index = range(1, len(display_df) + 1)
                
                st.dataframe(
                    display_df, 
                    use_container_width=True,
                    column_config={
                        "売上構成比": st.column_config.ProgressColumn(
                            "売上構成比",
                            format="%.1f%%",
                            min_value=0,
                            max_value=100,
                        ),
                        "合計売上": st.column_config.NumberColumn(format="¥%d"),
                        "Shopify売上": st.column_config.NumberColumn(format="¥%d"),
                        "Square売上": st.column_config.NumberColumn(format="¥%d"),
                    }
                )
        else:
            st.info("No product data available.")

    finally:
        con.close()
