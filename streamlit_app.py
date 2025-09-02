#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify x GA4 x Square x Google Ads 統合ダッシュボード
実データ取得による本実装版

実行:
  streamlit run streamlit_app.py
"""

import os
import sys
import duckdb
import pandas as pd
import streamlit as st
import subprocess
from datetime import datetime, timedelta, date

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
        # mart_dailyが存在しない場合は空のDataFrameを返す
        st.warning("⚠️ データベーススキーマが初期化されていません。ETLスクリプトを実行してください。")
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
    page_title="Shopify x GA4 x Square x Google Ads Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# サイドバー設定
def render_sidebar():
    """サイドバーを表示"""
    st.sidebar.title("🎛️ 設定")
    
    # 最終更新日時
    st.sidebar.subheader("📅 最終更新")
    
    # DuckDBファイルの最終更新日時を取得
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_test.duckdb")
    if os.path.exists(db_path):
        last_modified = datetime.fromtimestamp(os.path.getmtime(db_path))
        st.sidebar.write(f"**データベース**: {last_modified.strftime('%Y-%m-%d %H:%M')}")
        
        # 36時間以上古い場合は警告
        if datetime.now() - last_modified > timedelta(hours=36):
            st.sidebar.warning("⚠️ データが36時間以上古いです")
    else:
        st.sidebar.warning("⚠️ データベースが見つかりません")
    
    st.sidebar.divider()
    
    # データ更新ボタン
    st.sidebar.subheader("🔄 データ更新")
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("スキーマ初期化"):
            st.sidebar.info("DuckDBスキーマを初期化します...")
            try:
                result = subprocess.run(["python", "scripts/bootstrap_duckdb.py"], 
                                      capture_output=True, text=True, cwd=os.getcwd())
                if result.returncode == 0:
                    st.sidebar.success("✅ スキーマ初期化完了")
                else:
                    st.sidebar.error(f"❌ エラー: {result.stderr}")
            except Exception as e:
                st.sidebar.error(f"❌ 実行エラー: {e}")
    
    with col2:
        if st.button("増分更新実行"):
            st.sidebar.info("データ更新を開始します...")
            try:
                result = subprocess.run(["python", "scripts/run_etl.py"], 
                                      capture_output=True, text=True, cwd=os.getcwd())
                if result.returncode == 0:
                    st.sidebar.success("✅ データ更新完了")
                else:
                    st.sidebar.error(f"❌ エラー: {result.stderr}")
            except Exception as e:
                st.sidebar.error(f"❌ 実行エラー: {e}")
    
    st.sidebar.divider()
    
    # 環境情報
    st.sidebar.subheader("ℹ️ 環境情報")
    st.sidebar.write(f"**Python**: {sys.version}")
    st.sidebar.write(f"**Streamlit**: {st.__version__}")
    
    # 設定ファイルの存在確認
    env_file = ".env"
    if os.path.exists(env_file):
        st.sidebar.success("✅ .env ファイルが存在します")
    else:
        st.sidebar.error("❌ .env ファイルが見つかりません")
    
    st.sidebar.divider()
    
    # ヘルプ
    st.sidebar.subheader("❓ ヘルプ")
    st.sidebar.markdown("""
    **使い方**:
    1. 各タブで期間を選択
    2. データが表示されない場合は「増分更新実行」
    3. 品質チェックタブでデータ状態を確認
    
    **トラブルシューティング**:
    - データが古い → 増分更新実行
    - エラーが発生 → 品質チェックタブで確認
    - 設定エラー → .env ファイルを確認
    """)


def main():
    """メインアプリケーション"""
    st.title("🚀 Shopify x GA4 x Square x Google Ads 統合ダッシュボード")
    st.markdown("**実データ取得による本実装版**")
    
    # サイドバーを表示
    render_sidebar()
    
    # 期間選択UI
    today = date.today()
    default_start = today - timedelta(days=30)
    start = st.sidebar.date_input("開始日", default_start)
    end = st.sidebar.date_input("終了日", today)
    
    # データ読み込み（軽量化）
    df = load_mart_daily(start, end)
    df_yoy = load_mart_yoy(start, end)
    
    if df.empty:
        st.error("❌ データベーススキーマが初期化されていません")
        st.info("以下の手順でデータを初期化してください：")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            **1. DuckDBスキーマ初期化**
            ```bash
            python scripts/bootstrap_duckdb.py
            ```
            """)
        
        with col2:
            st.markdown("""
            **2. ETL実行（実際のデータ取得）**
            ```bash
            python scripts/run_etl.py
            ```
            """)
        
        st.markdown("""
        **3. ヘルスチェック**
        ```bash
        python scripts/health_check.py
        ```
        """)
        
        st.stop()
    
    # タブ選択
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 KPIダッシュボード", 
        "🔍 詳細分析", 
        "📈 広告分析", 
        "🔍 品質チェック"
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
    **開発**: Cursor AI Assistant | **バージョン**: 2.0.0 | **最終更新**: 2025-09-02
    """)


if __name__ == "__main__":
    main()


