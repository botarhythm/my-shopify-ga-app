"""
広告分析タブ
Google Ads キャンペーン・キーワード分析
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import os
from datetime import datetime, timedelta


def get_db_connection():
    """DuckDB接続を取得（読取専用）"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
    return con


def load_campaign_data(start_date: str, end_date: str) -> pd.DataFrame:
    """キャンペーンデータを読み込み"""
    con = get_db_connection()
    try:
        query = """
        SELECT 
            date,
            campaign_id,
            campaign_name,
            SUM(cost) as cost,
            SUM(clicks) as clicks,
            SUM(impressions) as impressions,
            SUM(conversions) as conversions,
            SUM(conv_value) as conv_value
        FROM core_ads_campaign
        WHERE date BETWEEN ? AND ?
        GROUP BY date, campaign_id, campaign_name
        ORDER BY cost DESC
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    finally:
        con.close()


def load_keyword_data(start_date: str, end_date: str) -> pd.DataFrame:
    """キーワードデータを読み込み"""
    con = get_db_connection()
    try:
        query = """
        SELECT 
            date,
            campaign_id,
            campaign_name,
            SUM(cost) as cost,
            SUM(clicks) as clicks,
            SUM(impressions) as impressions,
            SUM(conversions) as conversions,
            SUM(conv_value) as conv_value
        FROM core_ads_campaign
        WHERE date BETWEEN ? AND ?
        GROUP BY date, campaign_id, campaign_name
        ORDER BY cost DESC
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    finally:
        con.close()


def render_campaign_analysis(df: pd.DataFrame):
    """キャンペーン分析を表示"""
    st.subheader("📈 広告パフォーマンス分析")
    
    if df.empty:
        st.warning("広告データが見つかりません")
        return
    
    # 日別サマリー
    daily_summary = df.groupby("date").agg({
        "cost": "sum",
        "clicks": "sum",
        "impressions": "sum",
        "conversions": "sum",
        "conv_value": "sum"
    }).reset_index()
    
    daily_summary["ctr"] = (daily_summary["clicks"] / daily_summary["impressions"]) * 100
    daily_summary["cvr"] = (daily_summary["conversions"] / daily_summary["clicks"]) * 100
    daily_summary["roas"] = daily_summary["conv_value"] / daily_summary["cost"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 費用・売上推移
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_summary["date"],
            y=daily_summary["cost"],
            mode="lines+markers",
            name="費用",
            line=dict(color="red")
        ))
        fig.add_trace(go.Scatter(
            x=daily_summary["date"],
            y=daily_summary["conv_value"],
            mode="lines+markers",
            name="売上",
            line=dict(color="green")
        ))
        fig.update_layout(
            title="費用・売上推移",
            xaxis_title="日付",
            yaxis_title="金額"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # ROAS推移
        fig = px.line(
            daily_summary,
            x="date",
            y="roas",
            title="ROAS推移",
            labels={"roas": "ROAS", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # 改善提案
    st.subheader("💡 広告改善提案")
    
    # 期間全体のサマリー
    total_cost = daily_summary["cost"].sum()
    total_revenue = daily_summary["conv_value"].sum()
    total_clicks = daily_summary["clicks"].sum()
    total_impressions = daily_summary["impressions"].sum()
    total_conversions = daily_summary["conversions"].sum()
    
    avg_ctr = (total_clicks / total_impressions) * 100 if total_impressions > 0 else 0
    avg_cvr = (total_conversions / total_clicks) * 100 if total_clicks > 0 else 0
    avg_roas = total_revenue / total_cost if total_cost > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("総費用", f"¥{total_cost:,.0f}")
    with col2:
        st.metric("総売上", f"¥{total_revenue:,.0f}")
    with col3:
        st.metric("平均CTR", f"{avg_ctr:.2f}%")
    with col4:
        st.metric("平均ROAS", f"{avg_roas:.2f}")
    
    # 改善提案
    if avg_roas < 1.5:
        st.warning("**ROAS改善が必要です** - 現在のROASが1.5を下回っています")
    
    if avg_ctr < 2.0:
        st.info("**CTR改善の余地があります** - クリエイティブの最適化を検討してください")
    
    if avg_cvr < 3.0:
        st.info("**CVR改善の余地があります** - ランディングページの最適化を検討してください")
    
    # 詳細テーブル
    st.subheader("📋 日別詳細")
    
    # 数値フォーマット
    display_df = daily_summary.copy()
    display_df["cost"] = display_df["cost"].apply(lambda x: f"¥{x:,.0f}")
    display_df["clicks"] = display_df["clicks"].apply(lambda x: f"{x:,}")
    display_df["impressions"] = display_df["impressions"].apply(lambda x: f"{x:,}")
    display_df["conversions"] = display_df["conversions"].apply(lambda x: f"{x:.2f}")
    display_df["conv_value"] = display_df["conv_value"].apply(lambda x: f"¥{x:,.0f}")
    display_df["ctr"] = display_df["ctr"].apply(lambda x: f"{x:.2f}%")
    display_df["cvr"] = display_df["cvr"].apply(lambda x: f"{x:.2f}%")
    display_df["roas"] = display_df["roas"].apply(lambda x: f"{x:.2f}")
    
    st.dataframe(display_df, use_container_width=True)


def render_keyword_analysis(df: pd.DataFrame):
    """キーワード分析を表示"""
    st.subheader("🔍 広告効率分析")
    
    if df.empty:
        st.warning("広告データが見つかりません")
        return
    
    # 日別サマリー（キーワード分析タブでも同じデータを使用）
    daily_summary = df.groupby("date").agg({
        "cost": "sum",
        "clicks": "sum",
        "impressions": "sum",
        "conversions": "sum",
        "conv_value": "sum"
    }).reset_index()
    
    daily_summary["ctr"] = (daily_summary["clicks"] / daily_summary["impressions"]) * 100
    daily_summary["cvr"] = (daily_summary["conversions"] / daily_summary["clicks"]) * 100
    daily_summary["roas"] = daily_summary["conv_value"] / daily_summary["cost"]
    daily_summary["cpc"] = daily_summary["cost"] / daily_summary["clicks"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # CTR推移
        fig = px.line(
            daily_summary,
            x="date",
            y="ctr",
            title="CTR推移",
            labels={"ctr": "CTR (%)", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # CVR推移
        fig = px.line(
            daily_summary,
            x="date",
            y="cvr",
            title="CVR推移",
            labels={"cvr": "CVR (%)", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # 効率指標サマリー
    st.subheader("📊 効率指標サマリー")
    
    avg_ctr = daily_summary["ctr"].mean()
    avg_cvr = daily_summary["cvr"].mean()
    avg_cpc = daily_summary["cpc"].mean()
    avg_roas = daily_summary["roas"].mean()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("平均CTR", f"{avg_ctr:.2f}%")
    with col2:
        st.metric("平均CVR", f"{avg_cvr:.2f}%")
    with col3:
        st.metric("平均CPC", f"¥{avg_cpc:.2f}")
    with col4:
        st.metric("平均ROAS", f"{avg_roas:.2f}")
    
    # 改善提案
    st.subheader("💡 効率改善提案")
    
    if avg_ctr < 2.0:
        st.warning("**CTR改善が必要です** - クリエイティブの最適化を検討してください")
    
    if avg_cvr < 3.0:
        st.info("**CVR改善の余地があります** - ランディングページの最適化を検討してください")
    
    if avg_cpc > 100:
        st.warning("**CPCが高すぎます** - 入札価格の見直しを検討してください")
    
    # 詳細テーブル
    st.subheader("📋 日別効率詳細")
    
    # 数値フォーマット
    display_df = daily_summary.copy()
    display_df["cost"] = display_df["cost"].apply(lambda x: f"¥{x:,.0f}")
    display_df["clicks"] = display_df["clicks"].apply(lambda x: f"{x:,}")
    display_df["impressions"] = display_df["impressions"].apply(lambda x: f"{x:,}")
    display_df["conversions"] = display_df["conversions"].apply(lambda x: f"{x:.2f}")
    display_df["conv_value"] = display_df["conv_value"].apply(lambda x: f"¥{x:,.0f}")
    display_df["ctr"] = display_df["ctr"].apply(lambda x: f"{x:.2f}%")
    display_df["cvr"] = display_df["cvr"].apply(lambda x: f"{x:.2f}%")
    display_df["roas"] = display_df["roas"].apply(lambda x: f"{x:.2f}")
    display_df["cpc"] = display_df["cpc"].apply(lambda x: f"¥{x:.2f}")
    
    st.dataframe(display_df, use_container_width=True)


def render_performance_trends(df: pd.DataFrame):
    """パフォーマンス推移を表示"""
    st.subheader("📊 パフォーマンス推移")
    
    if df.empty:
        st.warning("パフォーマンスデータが見つかりません")
        return
    
    # 日別サマリー
    daily_summary = df.groupby("date").agg({
        "cost": "sum",
        "clicks": "sum",
        "impressions": "sum",
        "conversions": "sum",
        "conv_value": "sum"
    }).reset_index()
    
    daily_summary["ctr"] = (daily_summary["clicks"] / daily_summary["impressions"]) * 100
    daily_summary["cvr"] = (daily_summary["conversions"] / daily_summary["clicks"]) * 100
    daily_summary["roas"] = daily_summary["conv_value"] / daily_summary["cost"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 費用・売上推移
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_summary["date"],
            y=daily_summary["cost"],
            mode="lines+markers",
            name="費用",
            line=dict(color="red")
        ))
        fig.add_trace(go.Scatter(
            x=daily_summary["date"],
            y=daily_summary["conv_value"],
            mode="lines+markers",
            name="売上",
            line=dict(color="green")
        ))
        fig.update_layout(
            title="費用・売上推移",
            xaxis_title="日付",
            yaxis_title="金額"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # ROAS推移
        fig = px.line(
            daily_summary,
            x="date",
            y="roas",
            title="ROAS推移",
            labels={"roas": "ROAS", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)


def render_ads_tab():
    """広告分析タブを表示"""
    st.header("📈 広告分析")
    
    # 日付範囲選択
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now(),
            key="ads_start"
        )
    
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime.now(),
            max_value=datetime.now(),
            key="ads_end"
        )
    
    if start_date > end_date:
        st.error("開始日は終了日より前である必要があります")
        return
    
    # タブ選択
    tab1, tab2, tab3 = st.tabs(["パフォーマンス分析", "効率分析", "推移分析"])
    
    with tab1:
        # キャンペーン分析
        campaign_df = load_campaign_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        render_campaign_analysis(campaign_df)
    
    with tab2:
        # キーワード分析
        keyword_df = load_keyword_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        render_keyword_analysis(keyword_df)
    
    with tab3:
        # パフォーマンス推移
        campaign_df = load_campaign_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        render_performance_trends(campaign_df)
