import streamlit as st
import plotly.express as px
from src.database.db import db

from src.ui.components.glossary import get_term_label, get_term_help

import pandas as pd
import plotly.graph_objects as go
from src.services.ads_recommendation import generate_recommendations, calculate_o2o_correlation

def render_ads_tab(start_date, end_date):
    st.header("📈 広告パフォーマンス & 次の一手")
    
    # --- Data Loading ---
    con = db.get_connection(read_only=True)
    try:
        # 1. Campaign Data
        query_ads = """
            SELECT 
                date,
                campaign_name,
                SUM(cost) as cost,
                SUM(clicks) as clicks,
                SUM(impressions) as impressions,
                SUM(conversions) as conversions,
                SUM(conversions_value) as value
            FROM core.ads_campaign
            WHERE date BETWEEN ? AND ?
            GROUP BY date, campaign_name
        """
        df_ads_daily = con.execute(query_ads, [start_date, end_date]).df()
        
        # 2. Square Sales Data (for O2O)
        query_square = """
            SELECT 
                date,
                SUM(square_sales) as square_sales
            FROM marts.daily_revenue
            WHERE date BETWEEN ? AND ?
            GROUP BY date
        """
        df_square = con.execute(query_square, [start_date, end_date]).df()
        
    finally:
        con.close()

    if df_ads_daily.empty:
        st.warning("指定期間の広告データがありません。")
        return

    # Aggregated for Campaign Table
    df_campaigns = df_ads_daily.groupby('campaign_name').agg({
        'cost': 'sum',
        'clicks': 'sum',
        'impressions': 'sum',
        'conversions': 'sum',
        'value': 'sum'
    }).reset_index()
    
    # Calculate Metrics
    df_campaigns['roas'] = df_campaigns.apply(lambda x: x['value'] / x['cost'] if x['cost'] > 0 else 0, axis=1)
    df_campaigns['cvr'] = df_campaigns.apply(lambda x: (x['conversions'] / x['clicks'] * 100) if x['clicks'] > 0 else 0, axis=1)
    df_campaigns['ctr'] = df_campaigns.apply(lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, axis=1)
    df_campaigns['cpa'] = df_campaigns.apply(lambda x: (x['cost'] / x['conversions']) if x['conversions'] > 0 else 0, axis=1)

    # --- 1. Next Steps (Recommendations) ---
    st.subheader("💡 次の一手 (AIアドバイス)")
    recommendations = generate_recommendations(df_campaigns)
    
    if recommendations:
        cols = st.columns(len(recommendations)) if len(recommendations) <= 3 else st.columns(3)
        for i, rec in enumerate(recommendations):
            col = cols[i % 3]
            with col:
                if rec['type'] == 'positive':
                    st.success(f"**{rec['title']}**\n\n{rec['message']}")
                elif rec['type'] == 'negative':
                    st.error(f"**{rec['title']}**\n\n{rec['message']}")
                else:
                    st.warning(f"**{rec['title']}**\n\n{rec['message']}")
    else:
        st.info("現在、特筆すべき改善提案はありません。順調に運用されています！")

    st.divider()

    # --- 2. Infographic Overview (Funnel & Cards) ---
    st.subheader("📊 全体パフォーマンス")
    
    # Total Metrics
    total_cost = df_campaigns['cost'].sum()
    total_conv = df_campaigns['conversions'].sum()
    total_value = df_campaigns['value'].sum()
    total_roas = total_value / total_cost if total_cost > 0 else 0
    
    # Metric Cards
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("💸 広告費", f"¥{int(total_cost):,}", help="使った広告予算の合計")
    m2.metric("💎 売上 (広告経由)", f"¥{int(total_value):,}", help="広告をクリックして発生した売上")
    m3.metric("🎯 コンバージョン数", f"{int(total_conv)}回", help="購入された回数")
    m4.metric("🚀 ROAS (費用対効果)", f"{total_roas:.2f}", delta="目標 4.0", delta_color="normal" if total_roas >= 4.0 else "inverse", help="広告費1円あたりの売上。4.0以上が理想。")

    # Funnel Chart
    st.markdown("##### 🔻 ユーザー行動ファネル")
    total_imp = df_campaigns['impressions'].sum()
    total_clicks = df_campaigns['clicks'].sum()
    
    fig_funnel = go.Figure(go.Funnel(
        y = ["表示回数", "クリック数", "購入 (CV)"],
        x = [total_imp, total_clicks, total_conv],
        textinfo = "value+percent initial",
        marker = {"color": ["#636EFA", "#EF553B", "#00CC96"]}
    ))
    fig_funnel.update_layout(height=300, margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig_funnel, use_container_width=True)

    st.divider()

    # --- 3. Campaign Details (Visual Table) ---
    st.subheader("📝 キャンペーン詳細")
    
    # Color styling for dataframe
    st.dataframe(
        df_campaigns,
        column_config={
            "campaign_name": "キャンペーン名",
            "cost": st.column_config.NumberColumn("費用", format="¥%d"),
            "clicks": st.column_config.NumberColumn("クリック", format="%d"),
            "ctr": st.column_config.NumberColumn("CTR (クリック率)", format="%.2f%%"),
            "cvr": st.column_config.NumberColumn("CVR (購入率)", format="%.2f%%"),
            "roas": st.column_config.ProgressColumn(
                "ROAS (費用対効果)",
                format="%.2f",
                min_value=0,
                max_value=10,
            ),
        },
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --- 4. O2O Analysis (Store Visits) ---
    st.subheader("🏪 店舗への波及効果 (O2O分析)")
    
    # Aggregate Ads by date for correlation
    df_ads_total_daily = df_ads_daily.groupby('date').agg({'cost': 'sum', 'clicks': 'sum'}).reset_index()
    
    o2o_result = calculate_o2o_correlation(df_ads_total_daily, df_square)
    
    st.markdown(o2o_result['message'])
    
    if 'merged_df' in o2o_result:
        df_o2o = o2o_result['merged_df']
        
        # Dual Axis Chart
        fig_o2o = go.Figure()
        
        # Bar: Ad Cost
        fig_o2o.add_trace(go.Bar(
            x=df_o2o['date'],
            y=df_o2o['cost'],
            name="広告費",
            marker_color='#EF553B',
            opacity=0.6
        ))
        
        # Line: Store Sales
        fig_o2o.add_trace(go.Scatter(
            x=df_o2o['date'],
            y=df_o2o['square_sales'],
            name="店舗売上 (Square)",
            yaxis='y2',
            line=dict(color='#00CC96', width=3)
        ))
        
        fig_o2o.update_layout(
            title="広告費と店舗売上の推移",
            yaxis=dict(title="広告費 (円)"),
            yaxis2=dict(title="店舗売上 (円)", overlaying='y', side='right'),
            legend=dict(x=0, y=1.1, orientation='h'),
            height=400
        )
        
        st.plotly_chart(fig_o2o, use_container_width=True)

