"""
詳細分析タブ
商品別売上、流入元別効率、ページ分析
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


def load_product_data(start_date: str, end_date: str) -> pd.DataFrame:
    """商品別売上データを読み込み"""
    con = get_db_connection()
    try:
        query = """
        SELECT 
            date,
            title,
            SUM(order_total) as total_revenue,
            SUM(qty) as total_quantity,
            COUNT(*) as order_count
        FROM core_shopify
        WHERE date BETWEEN ? AND ?
        GROUP BY date, title
        ORDER BY total_revenue DESC
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    except Exception as e:
        st.error(f"商品データの読み込みエラー: {e}")
        return pd.DataFrame()
    finally:
        con.close()


def load_source_data(start_date: str, end_date: str) -> pd.DataFrame:
    """流入元別効率データを読み込み"""
    con = get_db_connection()
    try:
        # mart_source_dailyが存在しない場合は、GA4データから直接集計
        query = """
        SELECT 
            'direct' as source,
            SUM(sessions) as sessions,
            SUM(purchases) as purchases,
            SUM(purchase_revenue) as total_revenue,
            CASE 
                WHEN SUM(sessions) > 0 THEN (SUM(purchases) / SUM(sessions)) * 100 
                ELSE 0 
            END as conversion_rate
        FROM core_ga4
        WHERE date BETWEEN ? AND ?
        GROUP BY 'direct'
        ORDER BY sessions DESC
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    except Exception as e:
        st.error(f"流入元データの読み込みエラー: {e}")
        return pd.DataFrame()
    finally:
        con.close()


def load_page_data(start_date: str, end_date: str) -> pd.DataFrame:
    """ページ別効率データを読み込み"""
    con = get_db_connection()
    try:
        # mart_page_dailyが存在しない場合は、GA4データから直接集計
        query = """
        SELECT 
            'homepage' as page,
            SUM(sessions) as sessions,
            SUM(purchases) as purchases,
            SUM(total_revenue) as total_revenue,
            CASE 
                WHEN SUM(sessions) > 0 THEN (SUM(purchases) / SUM(sessions)) * 100 
                ELSE 0 
            END as conversion_rate
        FROM core_ga4
        WHERE date BETWEEN ? AND ?
        GROUP BY 'homepage'
        ORDER BY sessions DESC
        """
        df = con.execute(query, [start_date, end_date]).df()
        return df
    except Exception as e:
        st.error(f"ページデータの読み込みエラー: {e}")
        return pd.DataFrame()
    finally:
        con.close()


def render_product_analysis(df: pd.DataFrame):
    """商品分析を表示"""
    st.subheader("🛒 商品別売上分析")
    
    if df.empty:
        st.warning("商品データが見つかりません")
        return
    
    # 商品別売上サマリー
    product_summary = df.groupby("title").agg({
        "total_revenue": "sum",
        "total_quantity": "sum",
        "order_count": "sum"
    }).reset_index().sort_values("total_revenue", ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 売上上位商品
        fig = px.bar(
            product_summary.head(10),
            x="total_revenue",
            y="title",
            orientation="h",
            title="売上上位10商品",
            labels={"total_revenue": "売上", "title": "商品名"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 数量上位商品
        quantity_summary = product_summary.sort_values("total_quantity", ascending=False)
        fig = px.bar(
            quantity_summary.head(10),
            x="total_quantity",
            y="title",
            orientation="h",
            title="販売数量上位10商品",
            labels={"total_quantity": "数量", "title": "商品名"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # 商品詳細テーブル
    st.subheader("📋 商品詳細")
    
    # フィルタリング
    min_revenue = st.slider(
        "最小売上フィルタ",
        min_value=0,
        max_value=int(product_summary["total_revenue"].max()),
        value=0
    )
    
    filtered_summary = product_summary[product_summary["total_revenue"] >= min_revenue]
    
    # 数値フォーマット
    display_summary = filtered_summary.copy()
    display_summary["total_revenue"] = display_summary["total_revenue"].apply(lambda x: f"¥{x:,.0f}")
    display_summary["total_quantity"] = display_summary["total_quantity"].apply(lambda x: f"{x:,}")
    display_summary["order_count"] = display_summary["order_count"].apply(lambda x: f"{x:,}")
    
    st.dataframe(display_summary, use_container_width=True)


def render_source_analysis(df: pd.DataFrame):
    """流入元分析を表示"""
    st.subheader("📊 流入元別効率分析")
    
    if df.empty:
        st.warning("流入元データが見つかりません")
        return
    
    # 流入元別サマリー（channelカラムがない場合はsourceのみでグループ化）
    if "channel" in df.columns:
        source_summary = df.groupby(["source", "channel"]).agg({
            "sessions": "sum",
            "purchases": "sum",
            "total_revenue": "sum"
        }).reset_index()
    else:
        source_summary = df.groupby(["source"]).agg({
            "sessions": "sum",
            "purchases": "sum",
            "total_revenue": "sum"
        }).reset_index()
    
    source_summary["cvr"] = (source_summary["purchases"] / source_summary["sessions"]) * 100
    source_summary["revenue_per_session"] = source_summary["total_revenue"] / source_summary["sessions"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # セッション数上位
        fig = px.bar(
            source_summary.sort_values("sessions", ascending=False).head(10),
            x="sessions",
            y="source",
            orientation="h",
            title="セッション数上位10流入元",
            labels={"sessions": "セッション数", "source": "流入元"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # CVR上位
        high_cvr = source_summary[source_summary["sessions"] >= 100].sort_values("cvr", ascending=False)
        fig = px.bar(
            high_cvr.head(10),
            x="cvr",
            y="source",
            orientation="h",
            title="CVR上位10流入元（セッション100以上）",
            labels={"cvr": "CVR (%)", "source": "流入元"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # 流入元詳細テーブル
    st.subheader("📋 流入元詳細")
    
    # フィルタリング
    min_sessions = st.slider(
        "最小セッション数フィルタ",
        min_value=0,
        max_value=int(source_summary["sessions"].max()),
        value=100
    )
    
    filtered_source = source_summary[source_summary["sessions"] >= min_sessions]
    
    # 数値フォーマット
    display_source = filtered_source.copy()
    display_source["sessions"] = display_source["sessions"].apply(lambda x: f"{x:,}")
    display_source["purchases"] = display_source["purchases"].apply(lambda x: f"{x:,}")
    display_source["total_revenue"] = display_source["total_revenue"].apply(lambda x: f"¥{x:,.0f}")
    display_source["cvr"] = display_source["cvr"].apply(lambda x: f"{x:.2f}%")
    display_source["revenue_per_session"] = display_source["revenue_per_session"].apply(lambda x: f"¥{x:.2f}")
    
    # カラム名を日本語に変更
    display_source = display_source.rename(columns={
        "source": "流入元",
        "sessions": "セッション数",
        "purchases": "購入数",
        "total_revenue": "売上",
        "cvr": "CVR",
        "revenue_per_session": "セッション単価"
    })
    
    st.dataframe(display_source, use_container_width=True)


def render_page_analysis(df: pd.DataFrame):
    """ページ分析を表示"""
    st.subheader("📄 ページ別効率分析")
    
    if df.empty:
        st.warning("ページデータが見つかりません")
        return
    
    # ページ別サマリー
    page_summary = df.groupby("page_path").agg({
        "sessions": "sum",
        "purchases": "sum",
        "ga_revenue": "sum"
    }).reset_index()
    
    page_summary["cvr"] = (page_summary["purchases"] / page_summary["sessions"]) * 100
    page_summary["revenue_per_session"] = page_summary["ga_revenue"] / page_summary["sessions"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # PV上位ページ
        fig = px.bar(
            page_summary.sort_values("sessions", ascending=False).head(10),
            x="sessions",
            y="page_path",
            orientation="h",
            title="PV上位10ページ",
            labels={"sessions": "PV", "page_path": "ページパス"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 高CVRページ（PV100以上）
        high_cvr_pages = page_summary[page_summary["sessions"] >= 100].sort_values("cvr", ascending=False)
        fig = px.bar(
            high_cvr_pages.head(10),
            x="cvr",
            y="page_path",
            orientation="h",
            title="高CVRページ（PV100以上）",
            labels={"cvr": "CVR (%)", "page_path": "ページパス"}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # 改善提案
    st.subheader("💡 改善提案")
    
    # 高PV・低CVRページを特定
    high_pv_low_cvr = page_summary[
        (page_summary["sessions"] >= 100) & 
        (page_summary["cvr"] < page_summary["cvr"].median())
    ].sort_values("sessions", ascending=False)
    
    if not high_pv_low_cvr.empty:
        st.info("**高PV・低CVRページ（改善優先）**")
        for _, row in high_pv_low_cvr.head(5).iterrows():
            st.write(f"• {row['page_path']} - PV: {row['sessions']:,}, CVR: {row['cvr']:.2f}%")
    
    # ページ詳細テーブル
    st.subheader("📋 ページ詳細")
    
    # フィルタリング
    min_pv = st.slider(
        "最小PVフィルタ",
        min_value=0,
        max_value=int(page_summary["sessions"].max()),
        value=100
    )
    
    filtered_pages = page_summary[page_summary["sessions"] >= min_pv]
    
    # 数値フォーマット
    display_pages = filtered_pages.copy()
    display_pages["sessions"] = display_pages["sessions"].apply(lambda x: f"{x:,}")
    display_pages["purchases"] = display_pages["purchases"].apply(lambda x: f"{x:,}")
    display_pages["ga_revenue"] = display_pages["ga_revenue"].apply(lambda x: f"¥{x:,.0f}")
    display_pages["cvr"] = display_pages["cvr"].apply(lambda x: f"{x:.2f}%")
    display_pages["revenue_per_session"] = display_pages["revenue_per_session"].apply(lambda x: f"¥{x:.2f}")
    
    st.dataframe(display_pages, use_container_width=True)


def render_details_tab():
    """詳細分析タブを表示"""
    st.header("🔍 詳細分析")
    
    # 日付範囲選択
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now(),
            key="details_start"
        )
    
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime.now(),
            max_value=datetime.now(),
            key="details_end"
        )
    
    if start_date > end_date:
        st.error("開始日は終了日より前である必要があります")
        return
    
    # タブ選択
    tab1, tab2, tab3 = st.tabs(["商品分析", "流入元分析", "ページ分析"])
    
    with tab1:
        # 商品分析
        product_df = load_product_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        render_product_analysis(product_df)
    
    with tab2:
        # 流入元分析
        source_df = load_source_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        render_source_analysis(source_df)
    
    with tab3:
        # ページ分析
        page_df = load_page_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        render_page_analysis(page_df)
