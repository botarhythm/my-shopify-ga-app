"""
KPIダッシュボードタブ
主要KPIの可視化とYoY比較
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import duckdb
import os
from datetime import datetime, timedelta


def get_db_connection():
    """DuckDB接続を取得"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    return duckdb.connect(db_path)


def load_kpi_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    KPIデータを読み込み
    
    Args:
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        DataFrame: KPIデータ
    """
    con = get_db_connection()
    try:
        query = """
        SELECT * FROM mart_daily_yoy
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    finally:
        con.close()


def calculate_kpi_summary(df: pd.DataFrame) -> dict:
    """
    KPIサマリーを計算
    
    Args:
        df: KPIデータ
    
    Returns:
        dict: KPIサマリー
    """
    if df.empty:
        return {}
    
    summary = {
        "total_revenue": df["total_revenue"].sum(),
        "total_sessions": df["sessions"].sum(),
        "total_purchases": df["purchases"].sum(),
        "total_cost": df["cost"].sum(),
        "avg_roas": df["roas"].mean(),
        "avg_cvr": (df["purchases"].sum() / df["sessions"].sum()) * 100 if df["sessions"].sum() > 0 else 0,
        "avg_aov": df["total_revenue"].sum() / df["purchases"].sum() if df["purchases"].sum() > 0 else 0,
    }
    
    # YoY比較
    if "total_revenue_prev" in df.columns:
        summary["revenue_yoy_pct"] = (
            (summary["total_revenue"] - df["total_revenue_prev"].sum()) / df["total_revenue_prev"].sum() * 100
        ) if df["total_revenue_prev"].sum() > 0 else 0
    
    return summary


def render_kpi_cards(summary: dict):
    """KPIカードを表示"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="総売上",
            value=f"¥{summary.get('total_revenue', 0):,.0f}",
            delta=f"{summary.get('revenue_yoy_pct', 0):+.1f}%" if "revenue_yoy_pct" in summary else None
        )
    
    with col2:
        st.metric(
            label="セッション数",
            value=f"{summary.get('total_sessions', 0):,}",
            delta=f"{summary.get('sessions_yoy_pct', 0):+.1f}%" if "sessions_yoy_pct" in summary else None
        )
    
    with col3:
        st.metric(
            label="コンバージョン率",
            value=f"{summary.get('avg_cvr', 0):.2f}%"
        )
    
    with col4:
        st.metric(
            label="ROAS",
            value=f"{summary.get('avg_roas', 0):.2f}",
            delta=f"{summary.get('roas_yoy_pct', 0):+.1f}%" if "roas_yoy_pct" in summary else None
        )


def render_revenue_trend(df: pd.DataFrame):
    """売上トレンドを表示"""
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("売上トレンド", "YoY比較"),
        vertical_spacing=0.1
    )
    
    # 売上トレンド
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["total_revenue"],
            mode="lines+markers",
            name="当年売上",
            line=dict(color="blue")
        ),
        row=1, col=1
    )
    
    if "total_revenue_prev" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["total_revenue_prev"],
                mode="lines+markers",
                name="前年同期",
                line=dict(color="gray", dash="dash")
            ),
            row=1, col=1
        )
    
    # YoY変化率
    if "revenue_yoy_pct" in df.columns:
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["revenue_yoy_pct"],
                name="YoY変化率",
                marker_color=df["revenue_yoy_pct"].apply(
                    lambda x: "green" if x > 0 else "red" if x < 0 else "gray"
                )
            ),
            row=2, col=1
        )
    
    fig.update_layout(
        height=600,
        showlegend=True,
        title_text="売上トレンドとYoY比較"
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_traffic_metrics(df: pd.DataFrame):
    """トラフィック指標を表示"""
    col1, col2 = st.columns(2)
    
    with col1:
        # セッション数
        fig = px.line(
            df, x="date", y="sessions",
            title="セッション数推移",
            labels={"sessions": "セッション数", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 購入数
        fig = px.line(
            df, x="date", y="purchases",
            title="購入数推移",
            labels={"purchases": "購入数", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)


def render_efficiency_metrics(df: pd.DataFrame):
    """効率指標を表示"""
    col1, col2 = st.columns(2)
    
    with col1:
        # ROAS
        fig = px.line(
            df, x="date", y="roas",
            title="ROAS推移",
            labels={"roas": "ROAS", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # CVR
        df_cvr = df.copy()
        df_cvr["cvr"] = (df_cvr["purchases"] / df_cvr["sessions"]) * 100
        
        fig = px.line(
            df_cvr, x="date", y="cvr",
            title="コンバージョン率推移",
            labels={"cvr": "CVR (%)", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)


def render_kpi_tab():
    """KPIタブを表示"""
    st.header("📊 KPIダッシュボード")
    
    # 日付範囲選択
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now()
        )
    
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime.now(),
            max_value=datetime.now()
        )
    
    if start_date > end_date:
        st.error("開始日は終了日より前である必要があります")
        return
    
    # データ読み込み
    df = load_kpi_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    
    if df.empty:
        st.warning("指定された期間のデータが見つかりません")
        return
    
    # KPIサマリー
    summary = calculate_kpi_summary(df)
    render_kpi_cards(summary)
    
    st.divider()
    
    # 売上トレンド
    render_revenue_trend(df)
    
    st.divider()
    
    # トラフィック指標
    st.subheader("📈 トラフィック指標")
    render_traffic_metrics(df)
    
    st.divider()
    
    # 効率指標
    st.subheader("⚡ 効率指標")
    render_efficiency_metrics(df)
    
    # データテーブル
    st.divider()
    st.subheader("📋 詳細データ")
    
    # 表示列を選択
    display_columns = [
        "date", "total_revenue", "sessions", "purchases", 
        "cost", "roas", "total_revenue_prev", "revenue_yoy_pct"
    ]
    
    available_columns = [col for col in display_columns if col in df.columns]
    display_df = df[available_columns].copy()
    
    # 数値列のフォーマット
    if "total_revenue" in display_df.columns:
        display_df["total_revenue"] = display_df["total_revenue"].apply(lambda x: f"¥{x:,.0f}")
    if "total_revenue_prev" in display_df.columns:
        display_df["total_revenue_prev"] = display_df["total_revenue_prev"].apply(lambda x: f"¥{x:,.0f}")
    if "cost" in display_df.columns:
        display_df["cost"] = display_df["cost"].apply(lambda x: f"¥{x:,.0f}")
    if "revenue_yoy_pct" in display_df.columns:
        display_df["revenue_yoy_pct"] = display_df["revenue_yoy_pct"].apply(lambda x: f"{x:+.1f}%")
    
    st.dataframe(display_df, use_container_width=True)
