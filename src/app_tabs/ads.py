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
    """DuckDB接続を取得"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    return duckdb.connect(db_path)


def load_campaign_data(start_date: str, end_date: str) -> pd.DataFrame:
    """キャンペーンデータを読み込み"""
    con = get_db_connection()
    try:
        query = """
        SELECT * FROM mart_campaign_daily
        WHERE date BETWEEN ? AND ?
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
        SELECT * FROM mart_keyword_daily
        WHERE date BETWEEN ? AND ?
        ORDER BY cost DESC
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    finally:
        con.close()


def render_campaign_analysis(df: pd.DataFrame):
    """キャンペーン分析を表示"""
    st.subheader("📈 キャンペーン分析")
    
    if df.empty:
        st.warning("キャンペーンデータが見つかりません")
        return
    
    # キャンペーン別サマリー
    campaign_summary = df.groupby(["campaign_id", "campaign_name"]).agg({
        "cost": "sum",
        "clicks": "sum",
        "impressions": "sum",
        "conversions": "sum",
        "conversions_value": "sum"
    }).reset_index()
    
    campaign_summary["ctr"] = (campaign_summary["clicks"] / campaign_summary["impressions"]) * 100
    campaign_summary["cvr"] = (campaign_summary["conversions"] / campaign_summary["clicks"]) * 100
    campaign_summary["roas"] = campaign_summary["conversions_value"] / campaign_summary["cost"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 費用上位キャンペーン
        fig = px.bar(
            campaign_summary.head(10),
            x="cost",
            y="campaign_name",
            orientation="h",
            title="費用上位10キャンペーン",
            labels={"cost": "費用", "campaign_name": "キャンペーン名"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # ROAS上位キャンペーン（費用1000円以上）
        high_roas = campaign_summary[campaign_summary["cost"] >= 1000].sort_values("roas", ascending=False)
        fig = px.bar(
            high_roas.head(10),
            x="roas",
            y="campaign_name",
            orientation="h",
            title="ROAS上位10キャンペーン（費用1000円以上）",
            labels={"roas": "ROAS", "campaign_name": "キャンペーン名"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # 改善提案
    st.subheader("💡 キャンペーン改善提案")
    
    # 低ROAS・高費用キャンペーン
    low_roas_high_cost = campaign_summary[
        (campaign_summary["cost"] >= 1000) & 
        (campaign_summary["roas"] < 1.5)
    ].sort_values("cost", ascending=False)
    
    if not low_roas_high_cost.empty:
        st.warning("**低ROAS・高費用キャンペーン（改善優先）**")
        for _, row in low_roas_high_cost.head(5).iterrows():
            st.write(f"• {row['campaign_name']} - 費用: ¥{row['cost']:,.0f}, ROAS: {row['roas']:.2f}")
    
    # 高CVR・低CTRキャンペーン（クリエイティブ改善候補）
    high_cvr_low_ctr = campaign_summary[
        (campaign_summary["impressions"] >= 1000) & 
        (campaign_summary["cvr"] > campaign_summary["cvr"].median()) &
        (campaign_summary["ctr"] < campaign_summary["ctr"].median())
    ].sort_values("cvr", ascending=False)
    
    if not high_cvr_low_ctr.empty:
        st.info("**高CVR・低CTRキャンペーン（クリエイティブ改善候補）**")
        for _, row in high_cvr_low_ctr.head(3).iterrows():
            st.write(f"• {row['campaign_name']} - CTR: {row['ctr']:.2f}%, CVR: {row['cvr']:.2f}%")
    
    # キャンペーン詳細テーブル
    st.subheader("📋 キャンペーン詳細")
    
    # フィルタリング
    min_cost = st.slider(
        "最小費用フィルタ",
        min_value=0,
        max_value=int(campaign_summary["cost"].max()),
        value=1000
    )
    
    filtered_campaigns = campaign_summary[campaign_summary["cost"] >= min_cost]
    
    # 数値フォーマット
    display_campaigns = filtered_campaigns.copy()
    display_campaigns["cost"] = display_campaigns["cost"].apply(lambda x: f"¥{x:,.0f}")
    display_campaigns["clicks"] = display_campaigns["clicks"].apply(lambda x: f"{x:,}")
    display_campaigns["impressions"] = display_campaigns["impressions"].apply(lambda x: f"{x:,}")
    display_campaigns["conversions"] = display_campaigns["conversions"].apply(lambda x: f"{x:.2f}")
    display_campaigns["conversions_value"] = display_campaigns["conversions_value"].apply(lambda x: f"¥{x:,.0f}")
    display_campaigns["ctr"] = display_campaigns["ctr"].apply(lambda x: f"{x:.2f}%")
    display_campaigns["cvr"] = display_campaigns["cvr"].apply(lambda x: f"{x:.2f}%")
    display_campaigns["roas"] = display_campaigns["roas"].apply(lambda x: f"{x:.2f}")
    
    st.dataframe(display_campaigns, use_container_width=True)


def render_keyword_analysis(df: pd.DataFrame):
    """キーワード分析を表示"""
    st.subheader("🔍 キーワード分析")
    
    if df.empty:
        st.warning("キーワードデータが見つかりません")
        return
    
    # キーワード別サマリー
    keyword_summary = df.groupby(["keyword", "campaign_name", "ad_group_name"]).agg({
        "cost": "sum",
        "clicks": "sum",
        "impressions": "sum",
        "conversions": "sum",
        "conversions_value": "sum"
    }).reset_index()
    
    keyword_summary["ctr"] = (keyword_summary["clicks"] / keyword_summary["impressions"]) * 100
    keyword_summary["cvr"] = (keyword_summary["conversions"] / keyword_summary["clicks"]) * 100
    keyword_summary["roas"] = keyword_summary["conversions_value"] / keyword_summary["cost"]
    keyword_summary["cpc"] = keyword_summary["cost"] / keyword_summary["clicks"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 費用上位キーワード
        fig = px.bar(
            keyword_summary.head(10),
            x="cost",
            y="keyword",
            orientation="h",
            title="費用上位10キーワード",
            labels={"cost": "費用", "keyword": "キーワード"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # ROAS上位キーワード（費用100円以上）
        high_roas_keywords = keyword_summary[keyword_summary["cost"] >= 100].sort_values("roas", ascending=False)
        fig = px.bar(
            high_roas_keywords.head(10),
            x="roas",
            y="keyword",
            orientation="h",
            title="ROAS上位10キーワード（費用100円以上）",
            labels={"roas": "ROAS", "keyword": "キーワード"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # 改善提案
    st.subheader("💡 キーワード改善提案")
    
    # 除外候補キーワード（高コスト・低ROAS）
    exclude_candidates = keyword_summary[
        (keyword_summary["cost"] >= 500) & 
        (keyword_summary["roas"] < 1.0)
    ].sort_values("cost", ascending=False)
    
    if not exclude_candidates.empty:
        st.error("**除外候補キーワード（高コスト・低ROAS）**")
        for _, row in exclude_candidates.head(5).iterrows():
            st.write(f"• {row['keyword']} - 費用: ¥{row['cost']:,.0f}, ROAS: {row['roas']:.2f}")
    
    # 入札下げ候補キーワード（高CPC・低CVR）
    bid_down_candidates = keyword_summary[
        (keyword_summary["clicks"] >= 10) & 
        (keyword_summary["cpc"] > keyword_summary["cpc"].median()) &
        (keyword_summary["cvr"] < keyword_summary["cvr"].median())
    ].sort_values("cpc", ascending=False)
    
    if not bid_down_candidates.empty:
        st.warning("**入札下げ候補キーワード（高CPC・低CVR）**")
        for _, row in bid_down_candidates.head(5).iterrows():
            st.write(f"• {row['keyword']} - CPC: ¥{row['cpc']:.2f}, CVR: {row['cvr']:.2f}%")
    
    # キーワード詳細テーブル
    st.subheader("📋 キーワード詳細")
    
    # フィルタリング
    min_cost = st.slider(
        "最小費用フィルタ",
        min_value=0,
        max_value=int(keyword_summary["cost"].max()),
        value=100,
        key="keyword_min_cost"
    )
    
    filtered_keywords = keyword_summary[keyword_summary["cost"] >= min_cost]
    
    # 数値フォーマット
    display_keywords = filtered_keywords.copy()
    display_keywords["cost"] = display_keywords["cost"].apply(lambda x: f"¥{x:,.0f}")
    display_keywords["clicks"] = display_keywords["clicks"].apply(lambda x: f"{x:,}")
    display_keywords["impressions"] = display_keywords["impressions"].apply(lambda x: f"{x:,}")
    display_keywords["conversions"] = display_keywords["conversions"].apply(lambda x: f"{x:.2f}")
    display_keywords["conversions_value"] = display_keywords["conversions_value"].apply(lambda x: f"¥{x:,.0f}")
    display_keywords["ctr"] = display_keywords["ctr"].apply(lambda x: f"{x:.2f}%")
    display_keywords["cvr"] = display_keywords["cvr"].apply(lambda x: f"{x:.2f}%")
    display_keywords["roas"] = display_keywords["roas"].apply(lambda x: f"{x:.2f}")
    display_keywords["cpc"] = display_keywords["cpc"].apply(lambda x: f"¥{x:.2f}")
    
    st.dataframe(display_keywords, use_container_width=True)


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
        "conversions_value": "sum"
    }).reset_index()
    
    daily_summary["ctr"] = (daily_summary["clicks"] / daily_summary["impressions"]) * 100
    daily_summary["cvr"] = (daily_summary["conversions"] / daily_summary["clicks"]) * 100
    daily_summary["roas"] = daily_summary["conversions_value"] / daily_summary["cost"]
    
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
            y=daily_summary["conversions_value"],
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
    tab1, tab2, tab3 = st.tabs(["キャンペーン分析", "キーワード分析", "パフォーマンス推移"])
    
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
