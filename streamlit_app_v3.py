#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
統合ダッシュボードv3
4つのプラットフォームのデータを統合表示
"""

import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

# アプリタブをインポート
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

st.set_page_config(
    page_title="統合ダッシュボード v3", 
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=300, show_spinner=False)
def load_shopify_data(start_date, end_date):
    """Shopifyデータを直接APIから取得"""
    try:
        from src.connectors.shopify import fetch_orders_incremental
        
        start_iso = f"{start_date}T00:00:00Z"
        orders_df = fetch_orders_incremental(start_iso)
        
        if not orders_df.empty:
            # 指定期間のデータのみフィルタリング
            filtered_orders = orders_df[
                (orders_df['date'] >= start_date) & 
                (orders_df['date'] <= end_date)
            ]
            
            # 注文単位で集計
            order_summary = filtered_orders.groupby('order_id').agg({
                'order_total': 'first',
                'created_at': 'first',
                'financial_status': 'first',
                'subtotal_price': 'first',
                'total_discounts': 'first',
                'total_tax': 'first',
                'shipping_price': 'first'
            }).reset_index()
            
            return order_summary
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Shopify API エラー: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_square_data(start_date, end_date):
    """Squareデータを直接APIから取得"""
    try:
        from src.connectors.square import fetch_payments
        
        payments_df = fetch_payments(start_date, end_date)
        
        if not payments_df.empty:
            return payments_df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Square API エラー: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_ga4_data(start_date, end_date):
    """GA4データを修正版APIから取得"""
    try:
        from fix_ga4_api_error import fetch_ga4_compatible
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        result = fetch_ga4_compatible(start_str, end_str)
        
        if not result.empty:
            return result
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"GA4 API エラー: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_google_ads_data(start_date, end_date):
    """Google Adsデータを取得"""
    try:
        from src.ads.google_ads_client import create_google_ads_client
        from src.ads.fetch_ads import fetch_campaign_data
        
        client = create_google_ads_client()
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        campaign_data = fetch_campaign_data(client, start_str, end_str)
        
        if not campaign_data.empty:
            return campaign_data
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Google Ads API エラー: {e}")
        return pd.DataFrame()

def render_kpi_cards(shopify_data, square_data, ga4_data, ads_data):
    """KPIカードを表示"""
    
    # 売上計算
    shopify_revenue = shopify_data['order_total'].sum() if not shopify_data.empty else 0
    square_revenue = square_data['amount'].sum() if not square_data.empty else 0
    total_revenue = shopify_revenue + square_revenue
    
    # GA4計算
    ga4_sessions = ga4_data['sessions'].sum() if not ga4_data.empty else 0
    ga4_users = ga4_data['users'].sum() if not ga4_data.empty else 0
    ga4_revenue = ga4_data['revenue'].sum() if not ga4_data.empty else 0
    
    # Google Ads計算
    ads_cost = ads_data['cost_micros'].sum() / 1000000 if not ads_data.empty else 0
    ads_impressions = ads_data['impressions'].sum() if not ads_data.empty else 0
    ads_clicks = ads_data['clicks'].sum() if not ads_data.empty else 0
    
    # ROAS計算
    roas = total_revenue / ads_cost if ads_cost > 0 else 0
    
    # KPIカード表示
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="総売上",
            value=f"¥{total_revenue:,.0f}",
            delta=f"Shopify: ¥{shopify_revenue:,.0f} + Square: ¥{square_revenue:,.0f}"
        )
    
    with col2:
        st.metric(
            label="GA4セッション数",
            value=f"{ga4_sessions:,}",
            delta=f"ユーザー数: {ga4_users:,}"
        )
    
    with col3:
        st.metric(
            label="Google Ads費用",
            value=f"¥{ads_cost:,.0f}",
            delta=f"ROAS: {roas:.1f}"
        )
    
    with col4:
        st.metric(
            label="総取引数",
            value=f"{len(shopify_data) + len(square_data)}件",
            delta=f"Shopify: {len(shopify_data)}件 + Square: {len(square_data)}件"
        )

def render_revenue_breakdown(shopify_data, square_data):
    """売上内訳を表示"""
    st.subheader("売上内訳")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 売上データ
        revenue_data = {
            'プラットフォーム': ['Shopify', 'Square'],
            '売上': [
                shopify_data['order_total'].sum() if not shopify_data.empty else 0,
                square_data['amount'].sum() if not square_data.empty else 0
            ]
        }
        df_revenue = pd.DataFrame(revenue_data)
        
        # 円グラフ
        fig = px.pie(
            df_revenue, 
            values='売上', 
            names='プラットフォーム',
            title='売上比率'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 売上詳細テーブル
        st.write("売上詳細")
        st.dataframe(df_revenue, use_container_width=True)
        
        # 比率計算
        total = df_revenue['売上'].sum()
        if total > 0:
            shopify_ratio = df_revenue[df_revenue['プラットフォーム'] == 'Shopify']['売上'].iloc[0] / total * 100
            square_ratio = df_revenue[df_revenue['プラットフォーム'] == 'Square']['売上'].iloc[0] / total * 100
            
            st.write(f"**Shopify比率**: {shopify_ratio:.1f}%")
            st.write(f"**Square比率**: {square_ratio:.1f}%")

def render_traffic_analysis(ga4_data):
    """トラフィック分析を表示"""
    if ga4_data.empty:
        st.info("GA4データがありません")
        return
    
    st.subheader("トラフィック分析")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 日別セッション数
        daily_sessions = ga4_data.groupby('date')['sessions'].sum().reset_index()
        fig = px.line(
            daily_sessions, 
            x='date', 
            y='sessions',
            title='日別セッション数'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 日別ユーザー数
        daily_users = ga4_data.groupby('date')['users'].sum().reset_index()
        fig = px.line(
            daily_users, 
            x='date', 
            y='users',
            title='日別ユーザー数'
        )
        st.plotly_chart(fig, use_container_width=True)

def render_ad_performance(ads_data):
    """広告パフォーマンスを表示"""
    if ads_data.empty:
        st.info("Google Adsデータがありません")
        return
    
    st.subheader("広告パフォーマンス")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # キャンペーン別費用
        campaign_cost = ads_data.groupby('campaign_name')['cost_micros'].sum().reset_index()
        campaign_cost['cost'] = campaign_cost['cost_micros'] / 1000000
        
        fig = px.bar(
            campaign_cost.head(10), 
            x='campaign_name', 
            y='cost',
            title='キャンペーン別費用（上位10件）'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # CTR分析
        campaign_ctr = ads_data.groupby('campaign_name')['ctr'].mean().reset_index()
        
        fig = px.bar(
            campaign_ctr.head(10), 
            x='campaign_name', 
            y='ctr',
            title='キャンペーン別CTR（上位10件）'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

def render_data_quality_check(shopify_data, square_data, ga4_data, ads_data):
    """データ品質チェックを表示"""
    st.subheader("データ品質チェック")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**データ件数**")
        st.write(f"- Shopify: {len(shopify_data)}件")
        st.write(f"- Square: {len(square_data)}件")
        st.write(f"- GA4: {len(ga4_data)}件")
        st.write(f"- Google Ads: {len(ads_data)}件")
    
    with col2:
        st.write("**データ範囲**")
        if not shopify_data.empty:
            st.write(f"- Shopify: ¥{shopify_data['order_total'].min():,.0f} 〜 ¥{shopify_data['order_total'].max():,.0f}")
        if not square_data.empty:
            st.write(f"- Square: ¥{square_data['amount'].min():,.0f} 〜 ¥{square_data['amount'].max():,.0f}")
        if not ga4_data.empty:
            st.write(f"- GA4: {ga4_data['sessions'].min():,.0f} 〜 {ga4_data['sessions'].max():,.0f} セッション")

def main():
    """メインアプリケーション"""
    st.title("🚀 統合ダッシュボード v3")
    st.markdown("**4つのプラットフォーム統合表示**")
    
    # 期間選択UI
    today = date.today()
    # 2025年8月をデフォルトに設定
    default_start = date(2025, 8, 1)
    default_end = date(2025, 8, 31)
    start = st.sidebar.date_input("開始日", default_start)
    end = st.sidebar.date_input("終了日", default_end)
    
    # データ取得
    with st.spinner("データを取得中..."):
        shopify_data = load_shopify_data(start, end)
        square_data = load_square_data(start, end)
        ga4_data = load_ga4_data(start, end)
        ads_data = load_google_ads_data(start, end)
    
    # KPIカード表示
    render_kpi_cards(shopify_data, square_data, ga4_data, ads_data)
    
    # タブ表示
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "売上内訳", 
        "トラフィック分析", 
        "広告パフォーマンス", 
        "データ品質",
        "詳細データ"
    ])
    
    with tab1:
        render_revenue_breakdown(shopify_data, square_data)
    
    with tab2:
        render_traffic_analysis(ga4_data)
    
    with tab3:
        render_ad_performance(ads_data)
    
    with tab4:
        render_data_quality_check(shopify_data, square_data, ga4_data, ads_data)
    
    with tab5:
        st.subheader("詳細データ")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Shopifyデータ**")
            if not shopify_data.empty:
                st.dataframe(shopify_data, use_container_width=True)
            else:
                st.info("Shopifyデータがありません")
        
        with col2:
            st.write("**Squareデータ**")
            if not square_data.empty:
                st.dataframe(square_data, use_container_width=True)
            else:
                st.info("Squareデータがありません")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.write("**GA4データ**")
            if not ga4_data.empty:
                st.dataframe(ga4_data, use_container_width=True)
            else:
                st.info("GA4データがありません")
        
        with col4:
            st.write("**Google Adsデータ**")
            if not ads_data.empty:
                st.dataframe(ads_data, use_container_width=True)
            else:
                st.info("Google Adsデータがありません")
    
    # フッター
    st.divider()
    st.markdown("""
    ---
    **開発**: Cursor AI Assistant | **バージョン**: 3.0.0 | **最終更新**: 2025-09-03
    """)

if __name__ == "__main__":
    main()
