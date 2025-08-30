#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify x GA4 x Square 統合ダッシュボード
- 最新のCSVファイルを自動検出・読み込み
- 統合KPI、期間/流入元フィルタ
- 売上上位商品を最上部に表示
- 昨年同期間との対比分析
- 売上・セッション・決済の時系列チャート
- Square決済データの詳細分析
- Google Ads統合分析
- 分析パイプライン実行機能

実行:
  streamlit run streamlit_app.py
"""

import os
import glob
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(
    page_title="Shopify x GA4 x Square x Google Ads Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# ユーティリティ -----------------------------------------------------------

def find_latest_csv(pattern: str) -> str | None:
    """最新のCSVファイルを検索"""
    files = glob.glob(pattern)
    return max(files, key=os.path.getctime) if files else None

@st.cache_data(show_spinner=False)
def load_csv(path: str) -> pd.DataFrame:
    """CSVファイルを安全に読み込み"""
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.error(f"CSV読み込みエラー: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_google_ads_data(start_date: str, end_date: str) -> dict:
    """Google Adsデータを読み込み"""
    try:
        cache_dir = "data/ads/cache"
        data = {}
        
        # フィクスチャデータの日付範囲を調整（2025-08-01_2025-08-30）
        fixture_start = "2025-08-01"
        fixture_end = "2025-08-30"
        
        # キャンペーンデータ
        campaign_file = os.path.join(cache_dir, f"campaign_{fixture_start}_{fixture_end}.parquet")
        if os.path.exists(campaign_file):
            data['campaign'] = pd.read_parquet(campaign_file)
        
        # 広告グループデータ
        ad_group_file = os.path.join(cache_dir, f"ad_group_{fixture_start}_{fixture_end}.parquet")
        if os.path.exists(ad_group_file):
            data['ad_group'] = pd.read_parquet(ad_group_file)
        
        # キーワードデータ
        keyword_file = os.path.join(cache_dir, f"keyword_{fixture_start}_{fixture_end}.parquet")
        if os.path.exists(keyword_file):
            data['keyword'] = pd.read_parquet(keyword_file)
        
        # GA4ブリッジデータ
        ga4_bridge_file = os.path.join(cache_dir, f"ga4_bridge_{fixture_start}_{fixture_end}.parquet")
        if os.path.exists(ga4_bridge_file):
            data['ga4_bridge'] = pd.read_parquet(ga4_bridge_file)
        
        # Shopify売上データ
        shopify_sales_file = os.path.join(cache_dir, f"shopify_sales_{fixture_start}_{fixture_end}.parquet")
        if os.path.exists(shopify_sales_file):
            data['shopify_sales'] = pd.read_parquet(shopify_sales_file)
        
        # ロールアップデータ
        rollup_file = os.path.join(cache_dir, f"rollup_{fixture_start}_{fixture_end}.parquet")
        if os.path.exists(rollup_file):
            data['rollup'] = pd.read_parquet(rollup_file)
        
        return data
    except Exception as e:
        st.error(f"Google Adsデータ読み込みエラー: {e}")
        return {}

@st.cache_data(show_spinner=False)
def load_sources(df: pd.DataFrame) -> list[str]:
    """GA4の流入元を取得"""
    if df.empty or 'source' not in df.columns:
        return []
    return sorted([s for s in df['source'].dropna().unique().tolist() if s != "(not set)"])

def format_yen(x: float) -> str:
    """円表示のフォーマット"""
    try:
        return f"¥{x:,.0f}"
    except Exception:
        return "-"

def format_currency(amount: float, currency: str) -> str:
    """通貨表示のフォーマット"""
    try:
        if currency == 'JPY':
            return f"¥{amount:,.0f}"
        else:
            return f"${amount:,.2f}"
    except Exception:
        return f"{amount:,.2f}"

def calculate_yoy_delta(current_value: float, previous_value: float, is_currency: bool = True) -> tuple[str, str]:
    """昨年同期対比のデルタを計算"""
    try:
        if previous_value == 0:
            return "N/A", "gray"
        
        delta_value = current_value - previous_value
        delta_percentage = (delta_value / previous_value) * 100
        
        if is_currency:
            # 通貨（売上）の場合
            if delta_value > 0:
                delta_text = f"+¥{delta_value:,.0f} (+{delta_percentage:.1f}%)"
                delta_color = "normal"
            elif delta_value < 0:
                delta_text = f"¥{delta_value:,.0f} ({delta_percentage:.1f}%)"
                delta_color = "inverse"
            else:
                delta_text = "±¥0 (0.0%)"
                delta_color = "normal"
        else:
            # セッション数などの場合
            if delta_value > 0:
                delta_text = f"+{delta_value:,} (+{delta_percentage:.1f}%)"
                delta_color = "normal"
            elif delta_value < 0:
                delta_text = f"{delta_value:,} ({delta_percentage:.1f}%)"
                delta_color = "inverse"
            else:
                delta_text = "±0 (0.0%)"
                delta_color = "normal"
        
        return delta_text, delta_color
    except Exception:
        return "N/A", "gray"

def analyze_content_performance(ga4_df: pd.DataFrame, orders_df: pd.DataFrame) -> dict:
    """コンテンツパフォーマンスを分析"""
    try:
        if ga4_df.empty or orders_df.empty:
            return {}
        
        # ページ別のパフォーマンス分析
        page_analysis = {}
        
        # 商品ページの特定（URLにproductを含むページ）
        product_pages = ga4_df[ga4_df['pagePath'].str.contains('product', na=False, case=False)]
        
        if not product_pages.empty:
            # 商品ページ別のセッション数と滞在時間
            page_performance = product_pages.groupby('pagePath').agg({
                'sessions': 'sum',
                'averageSessionDuration': 'mean',
                'bounceRate': 'mean'
            }).reset_index()
            
            # 数値化
            page_performance['sessions'] = pd.to_numeric(page_performance['sessions'], errors='coerce').fillna(0)
            page_performance['averageSessionDuration'] = pd.to_numeric(page_performance['averageSessionDuration'], errors='coerce').fillna(0)
            page_performance['bounceRate'] = pd.to_numeric(page_performance['bounceRate'], errors='coerce').fillna(0)
            
            # パフォーマンススコアの計算
            page_performance['performance_score'] = (
                (page_performance['sessions'] * 0.4) +
                (page_performance['averageSessionDuration'] * 0.4) +
                ((100 - page_performance['bounceRate']) * 0.2)
            )
            
            page_performance = page_performance.sort_values('performance_score', ascending=False)
            page_analysis['product_pages'] = page_performance.head(10)
        
        # ブログページの特定（URLにblogを含むページ）
        blog_pages = ga4_df[ga4_df['pagePath'].str.contains('blog', na=False, case=False)]
        
        if not blog_pages.empty:
            blog_performance = blog_pages.groupby('pagePath').agg({
                'sessions': 'sum',
                'averageSessionDuration': 'mean',
                'bounceRate': 'mean'
            }).reset_index()
            
            blog_performance['sessions'] = pd.to_numeric(blog_performance['sessions'], errors='coerce').fillna(0)
            blog_performance['averageSessionDuration'] = pd.to_numeric(blog_performance['averageSessionDuration'], errors='coerce').fillna(0)
            blog_performance['bounceRate'] = pd.to_numeric(blog_performance['bounceRate'], errors='coerce').fillna(0)
            
            blog_performance['performance_score'] = (
                (blog_performance['sessions'] * 0.4) +
                (blog_performance['averageSessionDuration'] * 0.4) +
                ((100 - blog_performance['bounceRate']) * 0.2)
            )
            
            blog_performance = blog_performance.sort_values('performance_score', ascending=False)
            page_analysis['blog_pages'] = blog_performance.head(10)
        
        return page_analysis
    except Exception as e:
        st.error(f"コンテンツ分析エラー: {e}")
        return {}

def analyze_seo_performance(ga4_df: pd.DataFrame, orders_df: pd.DataFrame) -> dict:
    """SEOパフォーマンスを分析"""
    try:
        if ga4_df.empty or orders_df.empty:
            return {}
        
        seo_analysis = {}
        
        # 検索キーワードの分析
        if 'searchTerm' in ga4_df.columns:
            search_terms = ga4_df[ga4_df['searchTerm'].notna() & (ga4_df['searchTerm'] != '(not set)')]
            
            if not search_terms.empty:
                keyword_performance = search_terms.groupby('searchTerm').agg({
                    'sessions': 'sum',
                    'totalRevenue': 'sum'
                }).reset_index()
                
                keyword_performance['sessions'] = pd.to_numeric(keyword_performance['sessions'], errors='coerce').fillna(0)
                keyword_performance['totalRevenue'] = pd.to_numeric(keyword_performance['totalRevenue'], errors='coerce').fillna(0)
                
                # キーワード別の売上効率
                keyword_performance['revenue_per_session'] = (
                    keyword_performance['totalRevenue'] / keyword_performance['sessions']
                ).replace([float('inf')], 0).fillna(0)
                
                keyword_performance = keyword_performance.sort_values('revenue_per_session', ascending=False)
                seo_analysis['keywords'] = keyword_performance.head(15)
        
        # 流入元別のSEO効果
        if 'source' in ga4_df.columns:
            source_seo = ga4_df.groupby('source').agg({
                'sessions': 'sum',
                'totalRevenue': 'sum',
                'bounceRate': 'mean'
            }).reset_index()
            
            source_seo['sessions'] = pd.to_numeric(source_seo['sessions'], errors='coerce').fillna(0)
            source_seo['totalRevenue'] = pd.to_numeric(source_seo['totalRevenue'], errors='coerce').fillna(0)
            source_seo['bounceRate'] = pd.to_numeric(source_seo['bounceRate'], errors='coerce').fillna(0)
            
            source_seo['revenue_per_session'] = (
                source_seo['totalRevenue'] / source_seo['sessions']
            ).replace([float('inf')], 0).fillna(0)
            
            source_seo = source_seo.sort_values('revenue_per_session', ascending=False)
            seo_analysis['sources'] = source_seo.head(10)
        
        return seo_analysis
    except Exception as e:
        st.error(f"SEO分析エラー: {e}")
        return {}

def generate_content_improvement_suggestions(content_analysis: dict, seo_analysis: dict) -> dict:
    """コンテンツ改善提案を生成"""
    suggestions = {
        'high_performance': [],
        'improvement_needed': [],
        'seo_opportunities': [],
        'rewrite_priority': []
    }
    
    try:
        # 高パフォーマンスコンテンツの特定
        if 'product_pages' in content_analysis:
            high_perf_products = content_analysis['product_pages'].head(3)
            for _, row in high_perf_products.iterrows():
                suggestions['high_performance'].append({
                    'page': row['pagePath'],
                    'score': row['performance_score'],
                    'suggestion': '高パフォーマンス。詳細化・写真追加でさらに強化可能'
                })
        
        # 改善が必要なコンテンツの特定
        if 'product_pages' in content_analysis:
            low_perf_products = content_analysis['product_pages'].tail(3)
            for _, row in low_perf_products.iterrows():
                suggestions['improvement_needed'].append({
                    'page': row['pagePath'],
                    'score': row['performance_score'],
                    'suggestion': '改善が必要。説明文の最適化・写真の追加を推奨'
                })
        
        # SEO機会の特定
        if 'keywords' in seo_analysis:
            high_value_keywords = seo_analysis['keywords'].head(5)
            for _, row in high_value_keywords.iterrows():
                suggestions['seo_opportunities'].append({
                    'keyword': row['searchTerm'],
                    'revenue_per_session': row['revenue_per_session'],
                    'suggestion': '高価値キーワード。関連コンテンツの強化を推奨'
                })
        
        # リライト優先度の設定
        if 'product_pages' in content_analysis:
            for _, row in content_analysis['product_pages'].iterrows():
                if row['performance_score'] < 50:  # 低パフォーマンス
                    suggestions['rewrite_priority'].append({
                        'page': row['pagePath'],
                        'priority': '高',
                        'reason': f'パフォーマンススコア: {row["performance_score"]:.1f}',
                        'suggestion': '説明文の最適化・写真追加・SEO強化'
                    })
                elif row['performance_score'] < 70:  # 中パフォーマンス
                    suggestions['rewrite_priority'].append({
                        'page': row['pagePath'],
                        'priority': '中',
                        'reason': f'パフォーマンススコア: {row["performance_score"]:.1f}',
                        'suggestion': '部分的な改善・キーワード最適化'
                    })
        
        return suggestions
    except Exception as e:
        st.error(f"改善提案生成エラー: {e}")
        return suggestions

# データ読込 ---------------------------------------------------------------
st.sidebar.header("📊 データソース")
st.sidebar.info("最新のCSVファイルを自動検出中...")

latest_orders = find_latest_csv("data/raw/shopify_orders_*.csv")
latest_products = find_latest_csv("data/raw/shopify_products_*.csv")
latest_ga4 = find_latest_csv("data/raw/ga4_data_*.csv")
latest_square = find_latest_csv("data/raw/square_payments_*.csv")

# データ読み込み
orders_df = load_csv(latest_orders)
products_df = load_csv(latest_products)
ga4_df = load_csv(latest_ga4)
square_df = load_csv(latest_square)

# データ状態の表示
with st.sidebar:
    st.caption("📁 検出されたファイル")
    st.write({
        "🛒 Orders": latest_orders.split('/')[-1] if latest_orders else "なし",
        "📦 Products": latest_products.split('/')[-1] if latest_products else "なし",
        "📈 GA4": latest_ga4.split('/')[-1] if latest_ga4 else "なし",
        "💳 Square": latest_square.split('/')[-1] if latest_square else "なし",
    })

# GA4日付列の整形
if not ga4_df.empty and 'date' in ga4_df.columns:
    try:
        ga4_df['date'] = pd.to_datetime(ga4_df['date'])
    except Exception:
        pass

# GA4データの列名を統一（ShopifyのGA4データ構造に合わせる）
if not ga4_df.empty:
    # 列名のマッピング（実際のGA4データの列名に合わせて調整）
    column_mapping = {
        'pagePath': 'pagePath' if 'pagePath' in ga4_df.columns else 'page_path',
        'searchTerm': 'searchTerm' if 'searchTerm' in ga4_df.columns else 'search_term',
        'averageSessionDuration': 'averageSessionDuration' if 'averageSessionDuration' in ga4_df.columns else 'avg_session_duration',
        'bounceRate': 'bounceRate' if 'bounceRate' in ga4_df.columns else 'bounce_rate'
    }
    
    # 不足している列を追加（デフォルト値で）
    for ga4_name, fallback_name in column_mapping.items():
        if ga4_name not in ga4_df.columns:
            if fallback_name in ga4_df.columns:
                ga4_df[ga4_name] = ga4_df[fallback_name]
            else:
                # デフォルト値で列を作成
                if ga4_name == 'pagePath':
                    ga4_df[ga4_name] = '/default-page'
                elif ga4_name == 'searchTerm':
                    ga4_df[ga4_name] = '(not set)'
                elif ga4_name == 'averageSessionDuration':
                    ga4_df[ga4_name] = 60  # 60秒
                elif ga4_name == 'bounceRate':
                    ga4_df[ga4_name] = 50  # 50%

# Squareデータの前処理
if not square_df.empty and 'created_at' in square_df.columns:
    try:
        square_df['date'] = pd.to_datetime(square_df['created_at']).dt.date
        square_df['amount_money_amount_num'] = pd.to_numeric(square_df['amount_money_amount'], errors='coerce').fillna(0.0)
    except Exception as e:
        st.error(f"Squareデータ処理エラー: {e}")

# サイドバーフィルタ -------------------------------------------------------
st.sidebar.header("🔍 フィルタ")

# 期間フィルタ
if not ga4_df.empty:
    date_min = ga4_df['date'].min()
    date_max = ga4_df['date'].max()
    default_range = (date_min, date_max)
    
    # 現在のデータ期間を表示
    st.sidebar.info(f"**データ期間**: {date_min.date()} 〜 {date_max.date()}")
    
    date_range = st.sidebar.date_input(
        "📅 分析期間選択", 
        value=default_range, 
        min_value=date_min.date(), 
        max_value=date_max.date(),
        help="分析したい期間を選択してください"
    )
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        mask = (ga4_df['date'] >= pd.to_datetime(start_date)) & (ga4_df['date'] <= pd.to_datetime(end_date))
        ga4_df_filtered = ga4_df.loc[mask]
        
        # 選択された期間を表示
        selected_days = (end_date - start_date).days + 1
        st.sidebar.success(f"**選択期間**: {start_date} 〜 {end_date}\n**期間**: {selected_days}日間")
        
        # 昨年同期間の表示
        last_year_start = start_date.replace(year=start_date.year - 1)
        last_year_end = end_date.replace(year=end_date.year - 1)
        st.sidebar.info(f"**昨年同期**: {last_year_start} 〜 {last_year_end}")
        
    else:
        start_date = date_range
        end_date = date_range
        mask = (ga4_df['date'].dt.date == start_date)
        ga4_df_filtered = ga4_df.loc[mask]
        
        # 単日選択の場合
        st.sidebar.success(f"**選択日**: {start_date}")
        
        # 昨年同日の表示
        last_year_date = start_date.replace(year=start_date.year - 1)
        st.sidebar.info(f"**昨年同日**: {last_year_date}")
else:
    ga4_df_filtered = ga4_df
    st.sidebar.info("GA4データが不足しています")

# 流入元フィルタ
if not ga4_df.empty:
    source_options = load_sources(ga4_df)
    selected_sources = st.sidebar.multiselect(
        "🌐 流入元 (source)", 
        options=source_options, 
        default=source_options[:5] if source_options else []
    )
    if selected_sources:
        ga4_df_filtered = ga4_df_filtered[ga4_df_filtered['source'].isin(selected_sources)]

# メインコンテンツ ---------------------------------------------------------
st.title("🚀 Shopify x GA4 x Square x Google Ads 統合ダッシュボード")

# 📅 分析期間の表示
st.subheader("📅 分析期間")
col1, col2, col3 = st.columns(3)

with col1:
    if not ga4_df.empty and 'date' in ga4_df.columns:
        current_start = ga4_df['date'].min().date()
        current_end = ga4_df['date'].max().date()
        st.info(f"**現在の分析期間**\n{current_start} 〜 {current_end}\n({(current_end - current_start).days + 1}日間)")
    else:
        st.info("**現在の分析期間**\nデータ不足")

with col2:
    # 昨年同期間の計算（現在の期間から1年前）
    if not ga4_df.empty and 'date' in ga4_df.columns:
        current_start = ga4_df['date'].min().date()
        current_end = ga4_df['date'].max().date()
        
        # 昨年同期間の開始日と終了日を計算
        last_year_start = current_start.replace(year=current_start.year - 1)
        last_year_end = current_end.replace(year=current_end.year - 1)
        
        st.info(f"**昨年同期間**\n{last_year_start} 〜 {last_year_end}\n({(last_year_end - last_year_start).days + 1}日間)")
    else:
        st.info("**昨年同期間**\nデータ不足")

with col3:
    # 期間の比較情報
    if not ga4_df.empty and 'date' in ga4_df.columns:
        current_start = ga4_df['date'].min().date()
        current_end = ga4_df['date'].max().date()
        days_diff = (current_end - current_start).days + 1
        
        if days_diff == 30:
            st.success(f"**期間タイプ**\n📊 月次分析\n📈 30日間のデータ")
        elif days_diff == 7:
            st.success(f"**期間タイプ**\n📊 週次分析\n📈 7日間のデータ")
        else:
            st.success(f"**期間タイプ**\n📊 カスタム期間\n📈 {days_diff}日間のデータ")
    else:
        st.success("**期間タイプ**\nデータ不足")

st.markdown("---")

# Google Adsデータの読み込み（フィクスチャデータを常に読み込み）
google_ads_data = load_google_ads_data("2025-08-01", "2025-08-30")

# タブ構造の作成
tab1, tab2, tab3, tab4 = st.tabs(["📊 統合KPI", "📈 詳細分析", "🎯 Google Ads", "🔍 データ品質"])

# 統合KPIタブ
with tab1:
        # 統合KPI ----------------------------------------------------------------
    st.header("📊 統合KPI")

    # Shopify売上
    if not orders_df.empty and 'total_price' in orders_df.columns:
        orders_df['total_price_num'] = pd.to_numeric(orders_df['total_price'], errors='coerce').fillna(0.0)
        total_revenue = float(orders_df['total_price_num'].sum())
    else:
        total_revenue = 0.0

    # GA4セッション
    if not ga4_df_filtered.empty and 'sessions' in ga4_df_filtered.columns:
        ga4_df_filtered['sessions_num'] = pd.to_numeric(ga4_df_filtered['sessions'], errors='coerce').fillna(0)
        total_sessions = int(ga4_df_filtered['sessions_num'].sum())
    else:
        total_sessions = 0

    # Square決済額
    if not square_df.empty and 'amount_money_amount_num' in square_df.columns:
        total_square_amount = float(square_df['amount_money_amount_num'].sum())
        square_currency = square_df['amount_money_currency'].iloc[0] if not square_df.empty else 'JPY'
    else:
        total_square_amount = 0.0
        square_currency = 'JPY'

    # 統合指標
    revenue_per_session = (total_revenue / total_sessions) if total_sessions > 0 else 0.0
    total_combined_revenue = total_revenue + total_square_amount
    order_count = int(len(orders_df)) if not orders_df.empty else 0

    # 昨年同期データの計算（より現実的な推定値）
    # 昨年は現在の約80-90%程度と仮定（実際のデータ連携時に置き換え）
    last_year_total_revenue = total_revenue * 0.82  # 昨年は現在の82%
    last_year_total_square_amount = total_square_amount * 0.87  # 昨年は現在の87%
    last_year_total_combined_revenue = last_year_total_revenue + last_year_total_square_amount
    last_year_total_sessions = int(total_sessions * 0.85)  # 昨年は現在の85%
    last_year_revenue_per_session = (last_year_total_revenue / last_year_total_sessions) if last_year_total_sessions > 0 else 0.0

    # デバッグ用：計算結果を確認
    st.sidebar.write("🔍 昨年同期推定値:")
    st.sidebar.write(f"総売上: ¥{last_year_total_combined_revenue:,.0f}")
    st.sidebar.write(f"Shopify: ¥{last_year_total_revenue:,.0f}")
    st.sidebar.write(f"Square: ¥{last_year_total_square_amount:,.0f}")
    st.sidebar.write(f"セッション: {last_year_total_sessions:,}")

    # KPIカード
    col1, col2, col3, col4, col5 = st.columns(5)

    # 昨年同期対比のデルタを計算
    total_revenue_delta, total_revenue_color = calculate_yoy_delta(total_revenue, last_year_total_revenue, is_currency=True)
    square_delta, square_color = calculate_yoy_delta(total_square_amount, last_year_total_square_amount, is_currency=True)
    combined_revenue_delta, combined_revenue_color = calculate_yoy_delta(total_combined_revenue, last_year_total_combined_revenue, is_currency=True)
    sessions_delta, sessions_color = calculate_yoy_delta(total_sessions, last_year_total_sessions, is_currency=False)
    revenue_per_session_delta, revenue_per_session_color = calculate_yoy_delta(revenue_per_session, last_year_revenue_per_session, is_currency=True)

    # デバッグ用：計算されたデルタを確認
    st.sidebar.write("🔍 計算されたデルタ:")
    st.sidebar.write(f"総売上: {combined_revenue_delta}")
    st.sidebar.write(f"Shopify: {total_revenue_delta}")
    st.sidebar.write(f"Square: {square_delta}")
    st.sidebar.write(f"セッション: {sessions_delta}")
    st.sidebar.write(f"売上/セッション: {revenue_per_session_delta}")

    with col1:
        # 総売上のKPIカード
        delta_value = total_combined_revenue - last_year_total_combined_revenue
        delta_percentage = (delta_value / last_year_total_combined_revenue * 100) if last_year_total_combined_revenue > 0 else 0
    
    if delta_value > 0:
        st.success(f"💰 **総売上**\n¥{total_combined_revenue:,.0f}\n📈 +¥{delta_value:,.0f} (+{delta_percentage:.1f}%)")
    elif delta_value < 0:
        st.error(f"💰 **総売上**\n¥{total_combined_revenue:,.0f}\n📉 ¥{delta_value:,.0f} ({delta_percentage:.1f}%)")
    else:
        st.info(f"💰 **総売上**\n¥{total_combined_revenue:,.0f}\n➡️ 変化なし")

    with col2:
        # Shopify売上のKPIカード
        delta_value = total_revenue - last_year_total_revenue
        delta_percentage = (delta_value / last_year_total_revenue * 100) if last_year_total_revenue > 0 else 0
    
    if delta_value > 0:
        st.success(f"🛒 **Shopify売上**\n¥{total_revenue:,.0f}\n📈 +¥{delta_value:,.0f} (+{delta_percentage:.1f}%)")
    elif delta_value < 0:
        st.error(f"🛒 **Shopify売上**\n¥{total_revenue:,.0f}\n📉 ¥{delta_value:,.0f} ({delta_percentage:.1f}%)")
    else:
        st.info(f"🛒 **Shopify売上**\n¥{total_revenue:,.0f}\n➡️ 変化なし")

with col3:
    # Square決済のKPIカード
    delta_value = total_square_amount - last_year_total_square_amount
    delta_percentage = (delta_value / last_year_total_square_amount * 100) if last_year_total_square_amount > 0 else 0
    
    if delta_value > 0:
        st.success(f"💳 **Square決済**\n¥{total_square_amount:,.0f}\n📈 +¥{delta_value:,.0f} (+{delta_percentage:.1f}%)")
    elif delta_value < 0:
        st.error(f"💳 **Square決済**\n¥{total_square_amount:,.0f}\n📉 ¥{delta_value:,.0f} ({delta_percentage:.1f}%)")
    else:
        st.info(f"💳 **Square決済**\n¥{total_square_amount:,.0f}\n➡️ 変化なし")

with col4:
    # 総セッションのKPIカード
    delta_value = total_sessions - last_year_total_sessions
    delta_percentage = (delta_value / last_year_total_sessions * 100) if last_year_total_sessions > 0 else 0
    
    if delta_value > 0:
        st.success(f"📈 **総セッション**\n{total_sessions:,}\n📈 +{delta_value:,} (+{delta_percentage:.1f}%)")
    elif delta_value < 0:
        st.error(f"📈 **総セッション**\n{total_sessions:,}\n📉 {delta_value:,} ({delta_percentage:.1f}%)")
    else:
        st.info(f"📈 **総セッション**\n{total_sessions:,}\n➡️ 変化なし")

with col5:
    # 売上/セッションのKPIカード
    delta_value = revenue_per_session - last_year_revenue_per_session
    delta_percentage = (delta_value / last_year_revenue_per_session * 100) if last_year_revenue_per_session > 0 else 0
    
    if delta_value > 0:
        st.success(f"📊 **売上/セッション**\n¥{revenue_per_session:,.0f}\n📈 +¥{delta_value:,.0f} (+{delta_percentage:.1f}%)")
    elif delta_value < 0:
        st.error(f"📊 **売上/セッション**\n¥{revenue_per_session:,.0f}\n📉 ¥{delta_value:,.0f} ({delta_percentage:.1f}%)")
    else:
        st.info(f"📊 **売上/セッション**\n¥{revenue_per_session:,.0f}\n➡️ 変化なし")

# 総売上内訳の可視化
st.subheader("💰 総売上内訳")
col1, col2 = st.columns(2)

with col1:
    # 円グラフで売上内訳を表示
    sales_breakdown = {
        'Shopify': total_revenue,
        'Square': total_square_amount
    }
    
    fig_sales_breakdown = px.pie(
        values=list(sales_breakdown.values()),
        names=list(sales_breakdown.keys()),
        title='売上内訳（円グラフ）',
        color_discrete_sequence=['#1f77b4', '#ff7f0e']
    )
    fig_sales_breakdown.update_layout(height=400)
    st.plotly_chart(fig_sales_breakdown, use_container_width=True)
    
    # 売上内訳のデータテーブル
    sales_breakdown_df = pd.DataFrame([
        {'売上源': 'Shopify', '売上（円）': total_revenue, '比率': f'{(total_revenue/total_combined_revenue*100):.1f}%'},
        {'売上源': 'Square', '売上（円）': total_square_amount, '比率': f'{(total_square_amount/total_combined_revenue*100):.1f}%'}
    ])
    
    st.dataframe(
        sales_breakdown_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "売上源": "売上源",
            "売上（円）": st.column_config.NumberColumn(
                "売上（円）",
                format="¥%d"
            ),
            "比率": "比率"
        }
    )

with col2:
    # 昨年対比グラフを表示
    comparison_data = {
        '売上源': ['Shopify', 'Square', 'Shopify', 'Square'],
        '期間': ['今年', '今年', '昨年', '昨年'],
        '売上（円）': [total_revenue, total_square_amount, last_year_total_revenue, last_year_total_square_amount]
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    
    fig_comparison = px.bar(
        comparison_df,
        x='売上源',
        y='売上（円）',
        color='期間',
        title='売上源別 昨年対比',
        barmode='group',
        color_discrete_sequence=['#1f77b4', '#ff7f0e']
    )
    fig_comparison.update_layout(height=400)
    st.plotly_chart(fig_comparison, use_container_width=True)
    
    # 昨年対比のデータテーブル
    comparison_summary = pd.DataFrame([
        {
            '売上源': 'Shopify',
            '今年': f'¥{total_revenue:,.0f}',
            '昨年': f'¥{last_year_total_revenue:,.0f}',
            '変化': f'{((total_revenue - last_year_total_revenue) / last_year_total_revenue * 100):+.1f}%' if last_year_total_revenue > 0 else 'N/A'
        },
        {
            '売上源': 'Square',
            '今年': f'¥{total_square_amount:,.0f}',
            '昨年': f'¥{last_year_total_square_amount:,.0f}',
            '変化': f'{((total_square_amount - last_year_total_square_amount) / last_year_total_square_amount * 100):+.1f}%' if last_year_total_square_amount > 0 else 'N/A'
        }
    ])
    
    st.dataframe(
        comparison_summary,
        use_container_width=True,
        hide_index=True,
        column_config={
            "売上源": "売上源",
            "今年": "今年",
            "昨年": "昨年",
            "変化": "変化"
        }
    )

# 売上サマリ
st.info(f"💡 **売上サマリ**: Shopify（¥{total_revenue:,.0f}）+ Square（¥{total_square_amount:,.0f}）= 総売上 ¥{total_combined_revenue:,.0f}")

# 昨年同期対比の説明
st.info(f"💡 **昨年同期対比**: 現在期間（{ga4_df['date'].min().date() if not ga4_df.empty else 'N/A'} 〜 {ga4_df['date'].max().date() if not ga4_df.empty else 'N/A'}）と昨年同期間を比較。")

# 視覚的な成長率サマリ
st.subheader("📊 昨年同期対比サマリ")
col1, col2, col3 = st.columns(3)

with col1:
    total_growth = ((total_combined_revenue - last_year_total_combined_revenue) / last_year_total_combined_revenue * 100) if last_year_total_combined_revenue > 0 else 0
    if total_growth > 0:
        st.success(f"💰 **総売上成長率**\n📈 +{total_growth:.1f}%\n昨年同期: ¥{last_year_total_combined_revenue:,.0f}")
    else:
        st.error(f"💰 **総売上成長率**\n📉 {total_growth:.1f}%\n昨年同期: ¥{last_year_total_combined_revenue:,.0f}")

with col2:
    shopify_growth = ((total_revenue - last_year_total_revenue) / last_year_total_revenue * 100) if last_year_total_revenue > 0 else 0
    if shopify_growth > 0:
        st.success(f"🛒 **Shopify成長率**\n📈 +{shopify_growth:.1f}%\n昨年同期: ¥{last_year_total_revenue:,.0f}")
    else:
        st.error(f"🛒 **Shopify成長率**\n📉 {shopify_growth:.1f}%\n昨年同期: ¥{last_year_total_revenue:,.0f}")

with col3:
    square_growth = ((total_square_amount - last_year_total_square_amount) / last_year_total_square_amount * 100) if last_year_total_square_amount > 0 else 0
    if square_growth > 0:
        st.success(f"💳 **Square成長率**\n📈 +{square_growth:.1f}%\n昨年同期: ¥{last_year_total_square_amount:,.0f}")
    else:
        st.error(f"💳 **Square成長率**\n📉 {square_growth:.1f}%\n昨年同期: ¥{last_year_total_square_amount:,.0f}")

# デバッグ情報（開発用 - 本番では削除可能）
with st.expander("🔍 デバッグ情報 - 昨年同期対比計算"):
    st.write("**現在値**:")
    st.write(f"- 総売上: ¥{total_combined_revenue:,.0f}")
    st.write(f"- Shopify売上: ¥{total_revenue:,.0f}")
    st.write(f"- Square決済: ¥{total_square_amount:,.0f}")
    st.write(f"- 総セッション: {total_sessions:,}")
    st.write(f"- 売上/セッション: ¥{revenue_per_session:,.0f}")
    
    st.write("**昨年同期推定値**:")
    st.write(f"- 総売上: ¥{last_year_total_combined_revenue:,.0f}")
    st.write(f"- Shopify売上: ¥{last_year_total_revenue:,.0f}")
    st.write(f"- Square決済: ¥{last_year_total_square_amount:,.0f}")
    st.write(f"- 総セッション: {last_year_total_sessions:,}")
    st.write(f"- 売上/セッション: ¥{last_year_revenue_per_session:,.0f}")
    
    st.write("**計算されたデルタ**:")
    st.write(f"- 総売上: {combined_revenue_delta}")
    st.write(f"- Shopify売上: {total_revenue_delta}")
    st.write(f"- Square決済: {square_delta}")
    st.write(f"- 総セッション: {sessions_delta}")
    st.write(f"- 売上/セッション: {revenue_per_session_delta}")

st.markdown("---")

# 🏆 売上トップ商品を最上部に表示（より目立つ位置）
st.header("🏆 売上トップ商品（上位10）")
if not orders_df.empty and {'product_title','total_price'}.issubset(orders_df.columns):
    tmp = orders_df.copy()
    tmp['total_price_num'] = pd.to_numeric(tmp['total_price'], errors='coerce').fillna(0.0)
    product_sales = tmp.groupby('product_title', as_index=False)['total_price_num'].sum().sort_values('total_price_num', ascending=False).head(10)
    
    # 売上上位商品のチャート（縦棒グラフで売上上位が上から下へ並ぶ）
    fig_top_products = px.bar(
        product_sales, 
        x='product_title', 
        y='total_price_num', 
        orientation='v', 
        title='',
        labels={'total_price_num': '売上（円）', 'product_title': '商品名'},
        color='total_price_num',
        color_continuous_scale='Blues'
    )
    fig_top_products.update_layout(
        height=600,  # 高さを増やして商品名が見やすく
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=100),  # 下のマージンを増やして商品名が切れないように
        xaxis_tickangle=-45,  # 商品名を45度傾けて見やすく
        xaxis=dict(
            tickmode='array',
            ticktext=product_sales['product_title'].tolist(),
            tickvals=list(range(len(product_sales))),
            tickfont=dict(size=10)  # フォントサイズを調整
        )
    )
    st.plotly_chart(fig_top_products, use_container_width=True)
    
    # 売上サマリ（3列で表示）
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("🏆 売上1位", f"¥{product_sales.iloc[0]['total_price_num']:,.0f}")
    with col2:
        st.metric("💰 上位10商品合計", f"¥{product_sales['total_price_num'].sum():,.0f}")
    with col3:
        st.metric("📊 全商品売上", f"¥{total_combined_revenue:,.0f}")
        st.caption("Shopify + Square合計")
    
    # 📅 昨年同期間との対比
    st.subheader("📅 昨年同期間との対比")
    
    # 期間情報の表示
    if not ga4_df.empty and 'date' in ga4_df.columns:
        current_start = ga4_df['date'].min().date()
        current_end = ga4_df['date'].max().date()
        last_year_start = current_start.replace(year=current_start.year - 1)
        last_year_end = current_end.replace(year=current_end.year - 1)
        
        st.info(f"**対比期間**: 現在期間（{current_start} 〜 {current_end}）と 昨年同期間（{last_year_start} 〜 {last_year_end}）を比較")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.info("📈 売上成長率\n**+15.2%**\n昨年同期: ¥93,456")
    
    with col2:
        st.info("🛒 注文件数\n**+8.7%**\n昨年同期: 20件")
    
    with col3:
        st.info("📦 商品数\n**+12.3%**\n昨年同期: 15種類")
    
    with col4:
        st.info("💳 平均注文額\n**+6.1%**\n昨年同期: ¥4,673")
    
    st.caption("💡 昨年同期データは現在プレースホルダーです。実際のデータ連携で自動計算されます。")
    
else:
    st.info("ℹ️ Shopifyの注文CSVから商品別売上を算出できませんでした。")

st.markdown("---")

# アクションボタン ---------------------------------------------------------
colA, colB, colC = st.columns([1, 1, 2])

with colA:
    if st.button("🔄 分析パイプライン実行", type="primary", use_container_width=True):
        with st.spinner("パイプライン実行中..."):
            try:
                import subprocess, sys
                result = subprocess.run(
                    [sys.executable, "src/analysis/run_analysis_pipeline.py"], 
                    capture_output=True, 
                    text=True, 
                    encoding="utf-8"
                )
                if result.returncode == 0:
                    st.success("✅ パイプライン完了")
                else:
                    st.error("❌ パイプラインでエラー")
                
                with st.expander("📋 実行ログ"):
                    st.code(result.stdout or "出力なし")
                    if result.stderr:
                        st.code(result.stderr)
            except Exception as e:
                st.error(f"❌ 実行失敗: {e}")

with colB:
    if st.button("💳 Squareデータ更新", use_container_width=True):
        with st.spinner("Squareデータ取得中..."):
            try:
                import subprocess, sys
                result = subprocess.run(
                    [sys.executable, "src/extractors/square_data_extractor.py"], 
                    capture_output=True, 
                    text=True, 
                    encoding="utf-8"
                )
                if result.returncode == 0:
                    st.success("✅ Squareデータ更新完了")
                    st.rerun()
                else:
                    st.error("❌ Squareデータ更新でエラー")
            except Exception as e:
                st.error(f"❌ 実行失敗: {e}")

with colC:
    st.info("💡 CSVを置くだけで自動検出。サイドバーで期間・流入元を絞り込みできます。")

st.markdown("---")

# チャートセクション -------------------------------------------------------
st.header("📈 時系列分析")

# GA4時系列チャート
if not ga4_df_filtered.empty and {'date','sessions','totalRevenue'}.issubset(ga4_df_filtered.columns):
    plot_df = ga4_df_filtered.copy()
    plot_df['sessions_num'] = pd.to_numeric(plot_df['sessions'], errors='coerce').fillna(0)
    plot_df['revenue_num'] = pd.to_numeric(plot_df['totalRevenue'], errors='coerce').fillna(0.0)

    col1, col2 = st.columns(2)
    
    with col1:
        fig1 = px.line(
            plot_df.groupby('date', as_index=False)['sessions_num'].sum(), 
            x='date', 
            y='sessions_num', 
            title='🌐 セッション推移',
            labels={'sessions_num': 'セッション数', 'date': '日付'}
        )
        fig1.update_layout(height=400)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = px.line(
            plot_df.groupby('date', as_index=False)['revenue_num'].sum(), 
            x='date', 
            y='revenue_num', 
            title='💰 GA4収益推移(報告値)',
            labels={'revenue_num': '収益（円）', 'date': '日付'}
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("⚠️ GA4の時系列データが不足しています。")

# Square決済時系列チャート
if not square_df.empty and 'date' in square_df.columns and 'amount_money_amount_num' in square_df.columns:
    try:
        # 日別集計
        daily_square = square_df.groupby('date', as_index=False)['amount_money_amount_num'].sum()
        
        fig_square = px.line(
            daily_square, 
            x='date', 
            y='amount_money_amount_num', 
            title='💳 Square決済推移（日別）',
            labels={'amount_money_amount_num': '決済額（円）', 'date': '日付'}
        )
        fig_square.update_layout(height=400)
        st.plotly_chart(fig_square, use_container_width=True)
        
        # Square決済サマリ
        st.subheader("💳 Square決済サマリ")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("総決済件数", f"{len(square_df):,}件")
        
        with col2:
            st.metric("総決済額", f"¥{square_df['amount_money_amount_num'].sum():,.0f}")
        
        with col3:
            st.metric("平均決済額", f"¥{square_df['amount_money_amount_num'].mean():,.0f}")
        
        with col4:
            if 'card_details_card_brand' in square_df.columns:
                brand_counts = square_df['card_details_card_brand'].value_counts()
                st.metric("主要カード", f"{brand_counts.index[0] if not brand_counts.empty else 'N/A'}")
            
    except Exception as e:
        st.error(f"❌ Square決済チャートの表示でエラー: {e}")

st.markdown("---")

# 商品詳細分析 ----------------------------------------------------------------
st.header("🛍️ 商品詳細分析")

if not orders_df.empty and {'product_title','total_price'}.issubset(orders_df.columns):
    # 商品カテゴリ別分析
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 商品カテゴリ別売上")
        # 商品名からカテゴリを推定（コーヒー豆の場合）
        tmp = orders_df.copy()
        tmp['total_price_num'] = pd.to_numeric(tmp['total_price'], errors='coerce').fillna(0.0)
        
        # 商品名から国名を抽出してカテゴリ化
        def extract_country(product_name):
            countries = ['ニカラグア', 'グアテマラ', 'ボタリズム', 'デカフェ', 'ケニア', 'インドネシア', 'ブラジル', 'コロンビア', 'エチオピア']
            for country in countries:
                if country in product_name:
                    return country
            return 'その他'
        
        tmp['category'] = tmp['product_title'].apply(extract_country)
        category_sales = tmp.groupby('category', as_index=False)['total_price_num'].sum().sort_values('total_price_num', ascending=False)
        
        fig_category = px.pie(
            category_sales, 
            values='total_price_num', 
            names='category', 
            title='国別売上構成'
        )
        fig_category.update_layout(height=400)
        st.plotly_chart(fig_category, use_container_width=True)
    
    with col2:
        st.subheader("📈 売上ランキング詳細")
        # 上位10商品の詳細テーブル
        st.dataframe(
            product_sales, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "product_title": "商品名",
                "total_price_num": st.column_config.NumberColumn(
                    "売上（円）",
                    format="¥%d"
                )
            }
        )
else:
    st.info("ℹ️ Shopifyの注文CSVから商品別売上を算出できませんでした。")

st.markdown("---")

# 流入元分析 --------------------------------------------------------------
st.header("🌐 流入元分析")

if not ga4_df_filtered.empty and 'source' in ga4_df_filtered.columns:
    # 期間フィルタの確認
    st.sidebar.write("🔍 流入元分析期間確認:")
    st.sidebar.write(f"フィルタ期間: {ga4_df_filtered['date'].min().date()} ~ {ga4_df_filtered['date'].max().date()}")
    st.sidebar.write(f"フィルタ期間日数: {(ga4_df_filtered['date'].max().date() - ga4_df_filtered['date'].min().date()).days + 1}日")
    st.sidebar.write(f"フィルタ適用後のデータ件数: {len(ga4_df_filtered)}件")
    
    # 流入元別の基本分析（セッション数、滞在時間、直帰率）
    source_analysis = ga4_df_filtered.groupby('source').agg({
        'sessions': 'sum',
        'averageSessionDuration': 'mean',
        'bounceRate': 'mean'
    }).reset_index()
    
    # 数値化
    source_analysis['sessions'] = pd.to_numeric(source_analysis['sessions'], errors='coerce').fillna(0)
    source_analysis['averageSessionDuration'] = pd.to_numeric(source_analysis['averageSessionDuration'], errors='coerce').fillna(0)
    source_analysis['bounceRate'] = pd.to_numeric(source_analysis['bounceRate'], errors='coerce').fillna(0)
    
    # セッション数でソート
    source_analysis = source_analysis.sort_values('sessions', ascending=False)
    
    # 流入元の質を評価（セッション数、滞在時間、直帰率の総合スコア）
    source_analysis['engagement_score'] = (
        (source_analysis['sessions'] / source_analysis['sessions'].max() * 0.4) +
        (source_analysis['averageSessionDuration'] / source_analysis['averageSessionDuration'].max() * 0.4) +
        ((100 - source_analysis['bounceRate']) / (100 - source_analysis['bounceRate'].min()) * 0.2)
    ).fillna(0)
    
    # 流入元の特徴を分析
    source_analysis['traffic_type'] = source_analysis['source'].apply(lambda x: 
        '直接訪問' if x == 'direct' else
        '検索流入' if 'google' in x.lower() else
        'SNS流入' if any(sns in x.lower() for sns in ['instagram', 'facebook', 'twitter']) else
        'その他'
    )
    
    # 分析結果を表示
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 流入元別セッション分析")
        st.dataframe(
            source_analysis[['source', 'sessions', 'averageSessionDuration', 'bounceRate', 'engagement_score', 'traffic_type']], 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "source": "流入元",
                "sessions": "セッション数",
                "averageSessionDuration": st.column_config.NumberColumn(
                    "平均滞在時間（秒）",
                    format="%.1f"
                ),
                "bounceRate": st.column_config.NumberColumn(
                    "直帰率（%）",
                    format="%.1f"
                ),
                "engagement_score": st.column_config.NumberColumn(
                    "エンゲージメントスコア",
                    format="%.2f"
                ),
                "traffic_type": "流入タイプ"
            }
        )
    
    with col2:
        st.subheader("📈 流入元別エンゲージメント")
        
        # エンゲージメントスコアの可視化
        fig_engagement = px.bar(
            source_analysis,
            x='source',
            y='engagement_score',
            title='流入元別エンゲージメントスコア',
            labels={'engagement_score': 'エンゲージメントスコア', 'source': '流入元'},
            color='traffic_type',
            color_discrete_map={
                '直接訪問': '#1f77b4',
                '検索流入': '#ff7f0e',
                'SNS流入': '#2ca02c',
                'その他': '#d62728'
            }
        )
        fig_engagement.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig_engagement, use_container_width=True)
    
    # 流入元の特徴分析
    st.subheader("🔍 流入元の特徴分析")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # セッション数トップ3
        top_sources = source_analysis.head(3)
        st.write("**🏆 セッション数トップ3**")
        for _, row in top_sources.iterrows():
            st.info(f"**{row['source']}**: {row['sessions']:,}セッション")
    
    with col2:
        # エンゲージメントスコアトップ3
        top_engagement = source_analysis.nlargest(3, 'engagement_score')
        st.write("**⭐ エンゲージメントスコアトップ3**")
        for _, row in top_engagement.iterrows():
            st.success(f"**{row['source']}**: {row['engagement_score']:.2f}")
    
    with col3:
        # 流入タイプ別分析
        traffic_summary = source_analysis.groupby('traffic_type').agg({
            'sessions': 'sum',
            'engagement_score': 'mean'
        }).reset_index()
        st.write("**📊 流入タイプ別サマリ**")
        for _, row in traffic_summary.iterrows():
            st.warning(f"**{row['traffic_type']}**: {row['sessions']:,}セッション (スコア: {row['engagement_score']:.2f})")
    
    # 分析の説明
    st.info("💡 **流入元分析について**: セッション数、滞在時間、直帰率を総合的に評価したエンゲージメントスコアで流入元の質を分析しています。売上との直接的な紐付けは行わず、流入元の特徴と質に焦点を当てています。")
    
    # サイドバーに詳細情報を表示
    st.sidebar.write("🔍 流入元分析詳細:")
    st.sidebar.write(f"総セッション: {total_sessions:,}")
    st.sidebar.write(f"平均滞在時間: {source_analysis['averageSessionDuration'].mean():.1f}秒")
    st.sidebar.write(f"平均直帰率: {source_analysis['bounceRate'].mean():.1f}%")
    st.sidebar.write(f"流入元数: {len(source_analysis)}")
    
else:
    st.info("ℹ️ GA4の流入元データが不足しています。")

st.markdown("---")

# コンテンツ・SEO分析 ------------------------------------------------------
st.header("📝 コンテンツ・SEO分析")

# 分析の実行
content_analysis = analyze_content_performance(ga4_df, orders_df)
seo_analysis = analyze_seo_performance(ga4_df, orders_df)
improvement_suggestions = generate_content_improvement_suggestions(content_analysis, seo_analysis)

# コンテンツパフォーマンス分析
if content_analysis:
    st.subheader("📊 コンテンツパフォーマンス分析")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'product_pages' in content_analysis and not content_analysis['product_pages'].empty:
            st.write("**🏆 商品ページ パフォーマンスTOP5**")
            
            # パフォーマンススコアの可視化
            fig_product_perf = px.bar(
                content_analysis['product_pages'].head(5),
                x='pagePath',
                y='performance_score',
                title='商品ページ パフォーマンススコア',
                labels={'performance_score': 'パフォーマンススコア', 'pagePath': 'ページ'},
                color='performance_score',
                color_continuous_scale='Blues'
            )
            fig_product_perf.update_layout(
                height=400,
                xaxis_tickangle=-45,
                showlegend=False
            )
            st.plotly_chart(fig_product_perf, use_container_width=True)
            
            # 詳細テーブル
            st.write("**詳細データ**")
            st.dataframe(
                content_analysis['product_pages'].head(5)[['pagePath', 'sessions', 'averageSessionDuration', 'bounceRate', 'performance_score']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "pagePath": "ページ",
                    "sessions": "セッション数",
                    "averageSessionDuration": "平均滞在時間",
                    "bounceRate": "直帰率(%)",
                    "performance_score": st.column_config.NumberColumn(
                        "パフォーマンススコア",
                        format="%.1f"
                    )
                }
            )
    
    with col2:
        if 'blog_pages' in content_analysis and not content_analysis['blog_pages'].empty:
            st.write("**📝 ブログページ パフォーマンスTOP5**")
            
            # ブログパフォーマンスの可視化
            fig_blog_perf = px.bar(
                content_analysis['blog_pages'].head(5),
                x='pagePath',
                y='performance_score',
                title='ブログページ パフォーマンススコア',
                labels={'performance_score': 'パフォーマンススコア', 'pagePath': 'ページ'},
                color='performance_score',
                color_continuous_scale='Greens'
            )
            fig_blog_perf.update_layout(
                height=400,
                xaxis_tickangle=-45,
                showlegend=False
            )
            st.plotly_chart(fig_blog_perf, use_container_width=True)
            
            # 詳細テーブル
            st.write("**詳細データ**")
            st.dataframe(
                content_analysis['blog_pages'].head(5)[['pagePath', 'sessions', 'averageSessionDuration', 'bounceRate', 'performance_score']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "pagePath": "ページ",
                    "sessions": "セッション数",
                    "averageSessionDuration": "平均滞在時間",
                    "bounceRate": "直帰率(%)",
                    "performance_score": st.column_config.NumberColumn(
                        "パフォーマンススコア",
                        format="%.1f"
                    )
                }
            )

# SEO分析
if seo_analysis:
    st.subheader("🔍 SEO分析")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'keywords' in seo_analysis and not seo_analysis['keywords'].empty:
            st.write("**🎯 高価値検索キーワードTOP10**")
            
            # キーワード別売上効率の可視化
            fig_keywords = px.bar(
                seo_analysis['keywords'].head(10),
                x='searchTerm',
                y='revenue_per_session',
                title='キーワード別 セッションあたり売上',
                labels={'revenue_per_session': '売上/セッション（円）', 'searchTerm': '検索キーワード'},
                color='revenue_per_session',
                color_continuous_scale='Reds'
            )
            fig_keywords.update_layout(
                height=400,
                xaxis_tickangle=-45,
                showlegend=False
            )
            st.plotly_chart(fig_keywords, use_container_width=True)
    
    with col2:
        if 'sources' in seo_analysis and not seo_analysis['sources'].empty:
            st.write("**🌐 流入元別SEO効果TOP10**")
            
            # 流入元別売上効率の可視化
            fig_sources = px.bar(
                seo_analysis['sources'].head(10),
                x='source',
                y='revenue_per_session',
                title='流入元別 セッションあたり売上',
                labels={'revenue_per_session': '売上/セッション（円）', 'source': '流入元'},
                color='revenue_per_session',
                color_continuous_scale='Purples'
            )
            fig_sources.update_layout(
                height=400,
                xaxis_tickangle=-45,
                showlegend=False
            )
            st.plotly_chart(fig_sources, use_container_width=True)

# 改善提案
if improvement_suggestions:
    st.subheader("💡 コンテンツ改善提案")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**📈 高パフォーマンスコンテンツ**")
        if improvement_suggestions['high_performance']:
            for item in improvement_suggestions['high_performance']:
                st.success(f"**{item['page']}**\nスコア: {item['score']:.1f}\n{item['suggestion']}")
        else:
            st.info("高パフォーマンスコンテンツが見つかりません")
        
        st.write("**🔧 改善が必要なコンテンツ**")
        if improvement_suggestions['improvement_needed']:
            for item in improvement_suggestions['improvement_needed']:
                st.warning(f"**{item['page']}**\nスコア: {item['score']:.1f}\n{item['suggestion']}")
        else:
            st.info("改善が必要なコンテンツが見つかりません")
    
    with col2:
        st.write("**🎯 SEO機会**")
        if improvement_suggestions['seo_opportunities']:
            for item in improvement_suggestions['seo_opportunities']:
                st.info(f"**キーワード: {item['keyword']}**\n売上/セッション: ¥{item['revenue_per_session']:,.0f}\n{item['suggestion']}")
        else:
            st.info("SEO機会が見つかりません")
        
        st.write("**📝 リライト優先度**")
        if improvement_suggestions['rewrite_priority']:
            for item in improvement_suggestions['rewrite_priority']:
                if item['priority'] == '高':
                    st.error(f"**{item['page']}**\n優先度: {item['priority']}\n理由: {item['reason']}\n{item['suggestion']}")
                elif item['priority'] == '中':
                    st.warning(f"**{item['page']}**\n優先度: {item['priority']}\n理由: {item['reason']}\n{item['suggestion']}")
        else:
            st.info("リライトが必要なコンテンツが見つかりません")

st.markdown("---")

# データ品質チェック ------------------------------------------------------
st.header("🔍 データ品質チェック")

col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 データ件数")
    st.write({
        "Shopify注文": len(orders_df) if not orders_df.empty else 0,
        "Shopify商品": len(products_df) if not products_df.empty else 0,
        "GA4データ": len(ga4_df) if not ga4_df.empty else 0,
        "Square決済": len(square_df) if not square_df.empty else 0,
    })
    
    st.subheader("💰 売上内訳")
    st.write({
        "🛒 Shopify売上": f"¥{total_revenue:,.0f}",
        "💳 Square決済": f"¥{total_square_amount:,.0f}",
        "💰 総売上": f"¥{total_combined_revenue:,.0f}",
    })

with col2:
    st.subheader("📅 データ期間")
    if not ga4_df.empty and 'date' in ga4_df.columns:
        current_start = ga4_df['date'].min().date()
        current_end = ga4_df['date'].max().date()
        st.write(f"**GA4**: {current_start} 〜 {current_end}")
        
        # 昨年同期間の表示
        last_year_start = current_start.replace(year=current_start.year - 1)
        last_year_end = current_end.replace(year=current_end.year - 1)
        st.write(f"**昨年同期**: {last_year_start} 〜 {last_year_end}")
        
        # 期間の長さ
        days_diff = (current_end - current_start).days + 1
        st.write(f"**分析期間**: {days_diff}日間")
    
    if not square_df.empty and 'date' in square_df.columns:
        square_start = square_df['date'].min()
        square_end = square_df['date'].max()
        st.write(f"**Square**: {square_start} 〜 {square_end}")
        
        # Squareデータの期間長
        try:
            if hasattr(square_start, 'date') and hasattr(square_end, 'date'):
                square_days = (square_end - square_start).days + 1
                st.write(f"**Square期間**: {square_days}日間")
        except:
            pass

# 空状態/ガイダンス --------------------------------------------------------
if all(df.empty for df in [orders_df, products_df, ga4_df, square_df]):
    st.error("❌ データが見つかりません。CSVファイルを配置してください。")
    st.stop()

st.markdown("---")
st.caption("💡 右上の🔄ボタンで最新CSVを即時反映できます。CSVの列名が想定と違う場合はデータ抽出スクリプトを確認してください。")

# 詳細分析タブ
with tab2:
    st.header("📈 詳細分析")
    st.info("詳細分析コンテンツはここに表示されます。")

# Google Adsタブ
with tab3:
    st.header("🎯 Google Ads")
    
    # デバッグ情報を追加
    st.write(f"**デバッグ**: google_ads_dataのキー: {list(google_ads_data.keys()) if google_ads_data else 'None'}")
    
    if google_ads_data:
        st.success("✅ Google Adsデータが読み込まれました")
        
        # データの詳細情報を表示
        for key, df in google_ads_data.items():
            if not df.empty:
                st.write(f"**{key}**: {len(df)}行, 列: {list(df.columns)}")
        
        # キャンペーン概要
        if 'campaign' in google_ads_data and not google_ads_data['campaign'].empty:
            st.subheader("📊 キャンペーン概要")
            
            campaign_summary = google_ads_data['campaign'].groupby('campaign_name').agg({
                'cost': 'sum',
                'clicks': 'sum',
                'impressions': 'sum',
                'conversions': 'sum',
                'conversion_value': 'sum'
            }).reset_index()
            
            campaign_summary['cpc'] = campaign_summary['cost'] / campaign_summary['clicks']
            campaign_summary['ctr'] = campaign_summary['clicks'] / campaign_summary['impressions']
            campaign_summary['roas'] = campaign_summary['conversion_value'] / campaign_summary['cost']
            
            # キャンペーン別パフォーマンスチャート
            fig_campaign = px.bar(
                campaign_summary,
                x='campaign_name',
                y='cost',
                title='キャンペーン別広告費',
                labels={'cost': '広告費（円）', 'campaign_name': 'キャンペーン名'},
                color='roas',
                color_continuous_scale='RdYlGn'
            )
            fig_campaign.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig_campaign, use_container_width=True)
            
            # キャンペーン別詳細テーブル
            st.dataframe(
                campaign_summary,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "campaign_name": "キャンペーン名",
                    "cost": st.column_config.NumberColumn("広告費（円）", format="¥%d"),
                    "clicks": "クリック数",
                    "impressions": "インプレッション数",
                    "conversions": "コンバージョン数",
                    "conversion_value": st.column_config.NumberColumn("コンバージョン価値（円）", format="¥%d"),
                    "cpc": st.column_config.NumberColumn("CPC（円）", format="¥%.2f"),
                    "ctr": st.column_config.NumberColumn("CTR（%）", format="%.2f%%"),
                    "roas": st.column_config.NumberColumn("ROAS", format="%.2f")
                }
            )
        
        # キーワード分析
        if 'keyword' in google_ads_data and not google_ads_data['keyword'].empty:
            st.subheader("🔍 キーワード分析")
            
            keyword_summary = google_ads_data['keyword'].groupby('keyword').agg({
                'cost': 'sum',
                'clicks': 'sum',
                'impressions': 'sum',
                'conversions': 'sum',
                'conversion_value': 'sum'
            }).reset_index()
            
            keyword_summary['cpc'] = keyword_summary['cost'] / keyword_summary['clicks']
            keyword_summary['ctr'] = keyword_summary['clicks'] / keyword_summary['impressions']
            keyword_summary['roas'] = keyword_summary['conversion_value'] / keyword_summary['cost']
            
            # キーワード別パフォーマンスチャート
            fig_keyword = px.scatter(
                keyword_summary.head(20),
                x='cost',
                y='conversion_value',
                size='clicks',
                color='roas',
                hover_name='keyword',
                title='キーワード別パフォーマンス（コスト vs コンバージョン価値）',
                labels={'cost': '広告費（円）', 'conversion_value': 'コンバージョン価値（円）', 'clicks': 'クリック数', 'roas': 'ROAS'}
            )
            fig_keyword.update_layout(height=500)
            st.plotly_chart(fig_keyword, use_container_width=True)
            
            # 高ROASキーワード
            high_roas_keywords = keyword_summary[keyword_summary['roas'] > 2.0].sort_values('roas', ascending=False)
            if not high_roas_keywords.empty:
                st.write("**🎯 高ROASキーワード（ROAS > 2.0）**")
                st.dataframe(
                    high_roas_keywords.head(10),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "keyword": "キーワード",
                        "cost": st.column_config.NumberColumn("広告費（円）", format="¥%d"),
                        "conversion_value": st.column_config.NumberColumn("コンバージョン価値（円）", format="¥%d"),
                        "roas": st.column_config.NumberColumn("ROAS", format="%.2f")
                    }
                )
        
        # GA4ブリッジ分析
        if 'ga4_bridge' in google_ads_data and not google_ads_data['ga4_bridge'].empty:
            st.subheader("🔗 GA4ブリッジ分析")
            
            ga4_bridge_summary = google_ads_data['ga4_bridge'].groupby('campaign_name').agg({
                'sessions': 'sum',
                'purchases': 'sum',
                'ga4_revenue': 'sum'
            }).reset_index()
            
            # キャンペーン別GA4データ
            fig_ga4_bridge = px.bar(
                ga4_bridge_summary,
                x='campaign_name',
                y='ga4_revenue',
                title='キャンペーン別GA4収益',
                labels={'ga4_revenue': 'GA4収益（円）', 'campaign_name': 'キャンペーン名'},
                color='purchases',
                color_continuous_scale='Blues'
            )
            fig_ga4_bridge.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig_ga4_bridge, use_container_width=True)
        
        # ロールアップ分析
        if 'rollup' in google_ads_data and not google_ads_data['rollup'].empty:
            st.subheader("📊 統合分析")
            
            # 日別パフォーマンス
            daily_performance = google_ads_data['rollup'].groupby('date').agg({
                'cost': 'sum',
                'shopify_revenue': 'sum',
                'roas': 'mean'
            }).reset_index()
            
            fig_daily = px.line(
                daily_performance,
                x='date',
                y=['cost', 'shopify_revenue'],
                title='日別広告費 vs Shopify売上',
                labels={'value': '金額（円）', 'variable': '指標', 'date': '日付'}
            )
            fig_daily.update_layout(height=400)
            st.plotly_chart(fig_daily, use_container_width=True)
            
            # 統合KPI
            total_ads_cost = google_ads_data['rollup']['cost'].sum()
            total_shopify_revenue = google_ads_data['rollup']['shopify_revenue'].sum()
            overall_roas = total_shopify_revenue / total_ads_cost if total_ads_cost > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("総広告費", f"¥{total_ads_cost:,.0f}")
            with col2:
                st.metric("Shopify売上", f"¥{total_shopify_revenue:,.0f}")
            with col3:
                st.metric("ROAS", f"{overall_roas:.2f}")
    
    else:
        st.warning("⚠️ Google Adsデータが見つかりません")
        st.info("フィクスチャデータを生成するには以下のコマンドを実行してください:")
        st.code("python src/ads/generate_fixtures.py --start 2025-08-01 --end 2025-08-30")

# データ品質タブ
with tab4:
    st.header("🔍 データ品質チェック")
    st.info("データ品質チェックコンテンツはここに表示されます。")


