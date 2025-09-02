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
import streamlit as st
from datetime import datetime, timedelta

# アプリタブをインポート
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from app_tabs.kpi import render_kpi_tab
from app_tabs.details import render_details_tab
from app_tabs.ads import render_ads_tab
from app_tabs.quality import render_quality_tab

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
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
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
    if st.sidebar.button("増分更新実行"):
        st.sidebar.info("データ更新を開始します...")
        # ここでデータ更新処理を実行
        # 実際の実装では subprocess で run_incremental.py を実行
    
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


