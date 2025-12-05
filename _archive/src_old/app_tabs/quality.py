"""
品質チェックタブ
データ品質の監視と問題検出
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import os
from datetime import datetime, timedelta


def get_db_connection():
    """DuckDB接続を取得（読取専用）"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
    return con


def run_quality_checks() -> dict:
    """品質チェックを実行"""
    con = get_db_connection()
    results = {}
    
    try:
        # 欠損データチェック
        missing_data = con.execute("""
            SELECT 
                'mart_daily' AS table_name,
                date,
                'Missing sessions' AS issue
            FROM mart_daily 
            WHERE sessions IS NULL
            
            UNION ALL
            
            SELECT 
                'mart_daily' AS table_name,
                date,
                'Missing total_revenue' AS issue
            FROM mart_daily 
            WHERE total_revenue IS NULL
            
            UNION ALL
            
            SELECT 
                'mart_daily' AS table_name,
                date,
                'Missing cost' AS issue
            FROM mart_daily 
            WHERE cost IS NULL
        """).df()
        results["missing_data"] = missing_data
        
        # 異常値チェック
        anomalies = con.execute("""
            SELECT 
                date,
                sessions,
                LAG(sessions) OVER (ORDER BY date) AS prev_sessions,
                ABS(sessions - LAG(sessions) OVER (ORDER BY date)) AS change,
                'Large session change' AS issue
            FROM mart_daily
            WHERE ABS(sessions - LAG(sessions) OVER (ORDER BY date)) > 5 * STDDEV(sessions) OVER ()
        """).df()
        results["anomalies"] = anomalies
        
        # データ整合性チェック
        integrity_issues = con.execute("""
            SELECT 
                date,
                roas,
                'Negative ROAS' AS issue
            FROM mart_daily
            WHERE roas < 0
            
            UNION ALL
            
            SELECT 
                date,
                sessions,
                'Sessions less than purchases' AS issue
            FROM mart_daily
            WHERE sessions < purchases
        """).df()
        results["integrity_issues"] = integrity_issues
        
        # データ範囲チェック
        data_freshness = con.execute("""
            SELECT 
                MAX(date) AS latest_date,
                CURRENT_DATE - MAX(date) AS days_old,
                'Data too old' AS issue
            FROM mart_daily
            HAVING CURRENT_DATE - MAX(date) > 1
        """).df()
        results["data_freshness"] = data_freshness
        
        # 重複データチェック
        duplicates = con.execute("""
            SELECT 
                date,
                COUNT(*) AS duplicate_count,
                'Duplicate date records' AS issue
            FROM mart_daily
            GROUP BY date
            HAVING COUNT(*) > 1
        """).df()
        results["duplicates"] = duplicates
        
        # データ完全性チェック
        completeness = con.execute("""
            SELECT 
                'stg_ga4' AS table_name,
                COUNT(*) AS record_count,
                MIN(date) AS min_date,
                MAX(date) AS max_date
            FROM stg_ga4
            
            UNION ALL
            
            SELECT 
                'stg_shopify_orders' AS table_name,
                COUNT(*) AS record_count,
                MIN(created_at) AS min_date,
                MAX(created_at) AS max_date
            FROM stg_shopify_orders
            
            UNION ALL
            
            SELECT 
                'stg_square_payments' AS table_name,
                COUNT(*) AS record_count,
                MIN(created_at) AS min_date,
                MAX(created_at) AS max_date
            FROM stg_square_payments
            
            UNION ALL
            
            SELECT 
                'stg_ads_campaign' AS table_name,
                COUNT(*) AS record_count,
                MIN(date) AS min_date,
                MAX(date) AS max_date
            FROM stg_ads_campaign
        """).df()
        results["completeness"] = completeness
        
    finally:
        con.close()
    
    return results


def render_quality_summary(results: dict):
    """品質サマリーを表示"""
    st.subheader("📊 品質サマリー")
    
    # 問題の数をカウント
    total_issues = (
        len(results.get("missing_data", [])) +
        len(results.get("anomalies", [])) +
        len(results.get("integrity_issues", [])) +
        len(results.get("data_freshness", [])) +
        len(results.get("duplicates", []))
    )
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="総問題数",
            value=total_issues,
            delta=None
        )
    
    with col2:
        missing_count = len(results.get("missing_data", []))
        st.metric(
            label="欠損データ",
            value=missing_count,
            delta=None
        )
    
    with col3:
        anomaly_count = len(results.get("anomalies", []))
        st.metric(
            label="異常値",
            value=anomaly_count,
            delta=None
        )
    
    with col4:
        integrity_count = len(results.get("integrity_issues", []))
        st.metric(
            label="整合性問題",
            value=integrity_count,
            delta=None
        )
    
    # 品質ステータス
    if total_issues == 0:
        st.success("✅ データ品質は良好です")
    elif total_issues <= 5:
        st.warning("⚠️ 軽微な品質問題があります")
    else:
        st.error("❌ 重大な品質問題があります")


def render_missing_data_analysis(results: dict):
    """欠損データ分析を表示"""
    st.subheader("🔍 欠損データ分析")
    
    missing_data = results.get("missing_data", [])
    
    if missing_data.empty:
        st.success("✅ 欠損データはありません")
        return
    
    # 欠損データの詳細
    st.warning(f"⚠️ {len(missing_data)}件の欠損データが見つかりました")
    
    # 欠損データのテーブル
    st.dataframe(missing_data, use_container_width=True)
    
    # 欠損データの分布
    if not missing_data.empty:
        fig = px.histogram(
            missing_data,
            x="date",
            title="欠損データの日付分布",
            labels={"date": "日付", "count": "欠損件数"}
        )
        st.plotly_chart(fig, use_container_width=True)


def render_anomaly_analysis(results: dict):
    """異常値分析を表示"""
    st.subheader("📈 異常値分析")
    
    anomalies = results.get("anomalies", [])
    
    if anomalies.empty:
        st.success("✅ 異常値は検出されませんでした")
        return
    
    st.warning(f"⚠️ {len(anomalies)}件の異常値が見つかりました")
    
    # 異常値の詳細
    st.dataframe(anomalies, use_container_width=True)
    
    # 異常値の推移
    if not anomalies.empty:
        fig = px.scatter(
            anomalies,
            x="date",
            y="sessions",
            title="異常値の推移",
            labels={"date": "日付", "sessions": "セッション数"}
        )
        st.plotly_chart(fig, use_container_width=True)


def render_integrity_analysis(results: dict):
    """整合性分析を表示"""
    st.subheader("🔗 データ整合性分析")
    
    integrity_issues = results.get("integrity_issues", [])
    
    if integrity_issues.empty:
        st.success("✅ 整合性問題はありません")
        return
    
    st.error(f"❌ {len(integrity_issues)}件の整合性問題が見つかりました")
    
    # 整合性問題の詳細
    st.dataframe(integrity_issues, use_container_width=True)


def render_data_freshness_analysis(results: dict):
    """データ鮮度分析を表示"""
    st.subheader("⏰ データ鮮度分析")
    
    data_freshness = results.get("data_freshness", [])
    
    if data_freshness.empty:
        st.success("✅ データは最新です")
        return
    
    st.error("❌ データが古くなっています")
    
    # データ鮮度の詳細
    st.dataframe(data_freshness, use_container_width=True)


def render_completeness_analysis(results: dict):
    """データ完全性分析を表示"""
    st.subheader("📋 データ完全性分析")
    
    completeness = results.get("completeness", [])
    
    if completeness.empty:
        st.warning("⚠️ データ完全性情報が取得できませんでした")
        return
    
    # 各テーブルのレコード数
    fig = px.bar(
        completeness,
        x="table_name",
        y="record_count",
        title="各テーブルのレコード数",
        labels={"table_name": "テーブル名", "record_count": "レコード数"}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # データ範囲の可視化
    if not completeness.empty:
        # 日付範囲を可視化
        date_ranges = []
        for _, row in completeness.iterrows():
            if pd.notna(row["min_date"]) and pd.notna(row["max_date"]):
                date_ranges.append({
                    "table_name": row["table_name"],
                    "start_date": row["min_date"],
                    "end_date": row["max_date"]
                })
        
        if date_ranges:
            date_df = pd.DataFrame(date_ranges)
            fig = px.timeline(
                date_df,
                x_start="start_date",
                x_end="end_date",
                y="table_name",
                title="データ範囲",
                labels={"table_name": "テーブル名", "start_date": "開始日", "end_date": "終了日"}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # 完全性詳細テーブル
    st.dataframe(completeness, use_container_width=True)


def render_quality_tab():
    """品質チェックタブを表示"""
    st.header("🔍 データ品質チェック")
    
    # 品質チェック実行ボタン
    if st.button("🔄 品質チェック実行"):
        with st.spinner("品質チェックを実行中..."):
            results = run_quality_checks()
            st.session_state.quality_results = results
        st.success("品質チェックが完了しました")
    
    # 結果が保存されている場合は表示
    if "quality_results" in st.session_state:
        results = st.session_state.quality_results
        
        # 品質サマリー
        render_quality_summary(results)
        
        st.divider()
        
        # 詳細分析
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "欠損データ", "異常値", "整合性", "データ鮮度", "完全性"
        ])
        
        with tab1:
            render_missing_data_analysis(results)
        
        with tab2:
            render_anomaly_analysis(results)
        
        with tab3:
            render_integrity_analysis(results)
        
        with tab4:
            render_data_freshness_analysis(results)
        
        with tab5:
            render_completeness_analysis(results)
    
    else:
        st.info("「品質チェック実行」ボタンをクリックして品質チェックを開始してください")
