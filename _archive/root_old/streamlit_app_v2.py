#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify x GA4 x Square x Google Ads 統合ダッシュボード
修正版 - 直接API連携による統合

実行:
  streamlit run streamlit_app_v2.py
"""

import os
import sys
import duckdb
import pandas as pd
import streamlit as st
import subprocess
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

# アプリタブをインポート
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from app_tabs.kpi import render_kpi_tab
from app_tabs.details import render_details_tab
from app_tabs.ads import render_ads_tab
from app_tabs.quality import render_quality_tab

# DuckDB設定
DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_test.duckdb")

@st.cache_resource
def get_con_ro():
    """読取専用DuckDB接続（キャッシュ）"""
    con = duckdb.connect(DB, read_only=True)
    con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
    return con

def _df(sql: str, params: tuple|list=()):
    """SQL実行→Arrow→Pandas（軽量化）"""
    con = get_con_ro()
    return con.execute(sql, params).arrow().to_pandas()

@st.cache_data(ttl=300, show_spinner=False)
def load_shopify_data(start_date, end_date):
    """Shopifyデータを直接APIから取得"""
    try:
        from src.connectors.shopify import fetch_orders_incremental
        
        # 2025年8月のデータを取得
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
                'financial_status': 'first'
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

@st.cache_data(ttl=300, show_spinner=False)
def load_mart_daily(start, end):
    """日次マートデータ読み込み（キャッシュ）"""
    try:
        return _df("""
          SELECT *
          FROM mart_daily
          WHERE date BETWEEN ? AND ?
          ORDER BY date
        """, [str(start), str(end)])
    except duckdb.CatalogException:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def load_mart_yoy(start, end):
    """YoYデータ読み込み（キャッシュ・例外対応）"""
    try:
        return _df("""
          SELECT *
          FROM mart_daily_yoy
          WHERE date BETWEEN ? AND ?
          ORDER BY date
        """, [str(start), str(end)])
    except duckdb.CatalogException:
        return pd.DataFrame()

st.set_page_config(
    page_title="Shopify x GA4 x Square x Google Ads Dashboard v2", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# サイドバー設定
def render_sidebar():
    """サイドバーを表示"""
    st.sidebar.title("設定")
    
    # 最終更新日時
    st.sidebar.subheader("最終更新")
    
    # DuckDBファイルの最終更新日時を取得
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_test.duckdb")
    if os.path.exists(db_path):
        last_modified = datetime.fromtimestamp(os.path.getmtime(db_path))
        st.sidebar.write(f"**データベース**: {last_modified.strftime('%Y-%m-%d %H:%M')}")
        
        # 36時間以上古い場合は警告
        if datetime.now() - last_modified > timedelta(hours=36):
            st.sidebar.warning("データが36時間以上古いです")
    else:
        st.sidebar.warning("データベースが見つかりません")
    
    st.sidebar.divider()
    
    # データ更新ボタン
    st.sidebar.subheader("データ更新")
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("スキーマ初期化"):
            st.sidebar.info("DuckDBスキーマを初期化します...")
            try:
                result = subprocess.run(["python", "scripts/bootstrap_duckdb.py"], 
                                      capture_output=True, text=True, cwd=os.getcwd())
                if result.returncode == 0:
                    st.sidebar.success("スキーマ初期化完了")
                else:
                    st.sidebar.error(f"エラー: {result.stderr}")
            except Exception as e:
                st.sidebar.error(f"実行エラー: {e}")
    
    with col2:
        if st.button("増分更新実行"):
            st.sidebar.info("データ更新を開始します...")
            try:
                result = subprocess.run(["python", "scripts/run_etl.py"], 
                                      capture_output=True, text=True, cwd=os.getcwd())
                if result.returncode == 0:
                    st.sidebar.success("データ更新完了")
                else:
                    st.sidebar.error(f"エラー: {result.stderr}")
            except Exception as e:
                st.sidebar.error(f"実行エラー: {e}")
    
    st.sidebar.divider()
    
    # API接続状況
    st.sidebar.subheader("API接続状況")
    
    # 各APIの接続状況をチェック
    try:
        from test_integration import test_shopify_connection, test_square_connection, test_ga4_fixed, test_google_ads_setup
        
        shopify_ok = test_shopify_connection()
        square_ok = test_square_connection()
        ga4_ok = test_ga4_fixed()
        ads_ok = test_google_ads_setup()
        
        st.sidebar.write(f"**Shopify**: {'OK' if shopify_ok else 'ERROR'}")
        st.sidebar.write(f"**Square**: {'OK' if square_ok else 'ERROR'}")
        st.sidebar.write(f"**GA4**: {'OK' if ga4_ok else 'ERROR'}")
        st.sidebar.write(f"**Google Ads**: {'OK' if ads_ok else 'ERROR'}")
        
    except Exception as e:
        st.sidebar.error(f"API接続チェックエラー: {e}")
    
    st.sidebar.divider()
    
    # 環境情報
    st.sidebar.subheader("環境情報")
    st.sidebar.write(f"**Python**: {sys.version}")
    st.sidebar.write(f"**Streamlit**: {st.__version__}")
    
    # 設定ファイルの存在確認
    env_file = ".env"
    if os.path.exists(env_file):
        st.sidebar.success(".env ファイルが存在します")
    else:
        st.sidebar.error(".env ファイルが見つかりません")

def render_unified_dashboard(start_date, end_date):
    """統合ダッシュボードを表示"""
    st.header("統合ダッシュボード")
    
    # データ取得
    with st.spinner("データを取得中..."):
        shopify_data = load_shopify_data(start_date, end_date)
        square_data = load_square_data(start_date, end_date)
        ga4_data = load_ga4_data(start_date, end_date)
        ads_data = load_google_ads_data(start_date, end_date)
    
    # 売上サマリー
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if not shopify_data.empty:
            shopify_revenue = shopify_data['order_total'].sum()
            st.metric("Shopify売上", f"¥{shopify_revenue:,.0f}")
        else:
            st.metric("Shopify売上", "¥0")
    
    with col2:
        if not square_data.empty:
            square_revenue = square_data['amount'].sum()
            st.metric("Square売上", f"¥{square_revenue:,.0f}")
        else:
            st.metric("Square売上", "¥0")
    
    with col3:
        if not ga4_data.empty:
            ga4_sessions = ga4_data['sessions'].sum()
            st.metric("GA4セッション数", f"{ga4_sessions:,}")
        else:
            st.metric("GA4セッション数", "0")
    
    with col4:
        if not ads_data.empty:
            ads_cost = ads_data['cost_micros'].sum() / 1000000  # マイクロ単位から円に変換
            st.metric("Google Ads費用", f"¥{ads_cost:,.0f}")
        else:
            st.metric("Google Ads費用", "¥0")
    
    # 詳細データ表示
    st.subheader("詳細データ")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Shopify", "Square", "GA4", "Google Ads"])
    
    with tab1:
        if not shopify_data.empty:
            st.dataframe(shopify_data)
        else:
            st.info("Shopifyデータがありません")
    
    with tab2:
        if not square_data.empty:
            st.dataframe(square_data)
        else:
            st.info("Squareデータがありません")
    
    with tab3:
        if not ga4_data.empty:
            st.dataframe(ga4_data)
        else:
            st.info("GA4データがありません")
    
    with tab4:
        if not ads_data.empty:
            st.dataframe(ads_data)
        else:
            st.info("Google Adsデータがありません")

def main():
    """メインアプリケーション"""
    st.title("Shopify x GA4 x Square x Google Ads 統合ダッシュボード v2")
    st.markdown("**直接API連携による統合版**")
    
    # サイドバーを表示
    render_sidebar()
    
    # 期間選択UI
    today = date.today()
    # 2025年8月をデフォルトに設定
    default_start = date(2025, 8, 1)
    default_end = date(2025, 8, 31)
    start = st.sidebar.date_input("開始日", default_start)
    end = st.sidebar.date_input("終了日", default_end)
    
    # 統合ダッシュボードを表示
    render_unified_dashboard(start, end)
    
    # 従来のタブ（DuckDBベース）
    st.divider()
    st.subheader("従来のDuckDBベース分析")
    
    # データ読み込み（軽量化）
    df = load_mart_daily(start, end)
    df_yoy = load_mart_yoy(start, end)
    
    if df.empty:
        st.warning("DuckDBベースのデータがありません。直接API連携版をご利用ください。")
    else:
        # タブ選択
        tab1, tab2, tab3, tab4 = st.tabs([
            "KPIダッシュボード", 
            "詳細分析", 
            "広告分析", 
            "品質チェック"
        ])
        
        with tab1:
            render_kpi_tab()
        
        with tab2:
            render_details_tab()
        
        with tab3:
            render_ads_tab()
        
        with tab4:
            render_quality_tab()
    
    # フッター
    st.divider()
    st.markdown("""
    ---
    **開発**: Cursor AI Assistant | **バージョン**: 2.1.0 | **最終更新**: 2025-09-02
    """)

if __name__ == "__main__":
    main()