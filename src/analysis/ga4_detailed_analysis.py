import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path

def load_latest_ga4_data():
    """最新のGA4データを読み込み"""
    data_dir = Path("data/raw")
    ga4_files = list(data_dir.glob("ga4_data_*.csv"))
    
    if not ga4_files:
        return None
    
    # 最新のファイルを取得
    latest_file = max(ga4_files, key=lambda x: x.stat().st_mtime)
    print(f"📊 GA4データファイルを読み込み中: {latest_file.name}")
    
    df = pd.read_csv(latest_file)
    return df

def analyze_user_behavior(df):
    """ユーザー行動の詳細分析"""
    analysis = {}
    
    # 基本統計
    analysis['total_sessions'] = df['sessions'].sum()
    analysis['total_revenue'] = df['totalRevenue'].sum()
    analysis['avg_sessions_per_day'] = df.groupby('date')['sessions'].sum().mean()
    analysis['avg_revenue_per_session'] = analysis['total_revenue'] / analysis['total_sessions']
    
    # トラフィックソース分析
    source_analysis = df.groupby('source').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_values('sessions', ascending=False)
    
    source_analysis['revenue_per_session'] = source_analysis['totalRevenue'] / source_analysis['sessions']
    analysis['traffic_sources'] = source_analysis
    
    # ページ別分析
    page_analysis = df.groupby('pagePath').agg({
        'sessions_page': 'sum'
    }).sort_values('sessions_page', ascending=False).head(20)
    analysis['top_pages'] = page_analysis
    
    # 日別トレンド
    daily_trend = df.groupby('date').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_index()
    analysis['daily_trend'] = daily_trend
    
    return analysis

def analyze_customer_journey(df):
    """顧客ジャーニー分析"""
    journey_analysis = {}
    
    # ページ遷移パターン分析
    page_paths = df[df['pagePath'].notna()]['pagePath'].value_counts()
    journey_analysis['page_popularity'] = page_paths.head(15)
    
    # 商品ページ分析
    product_pages = df[df['pagePath'].str.contains('/products/', na=False)]
    if not product_pages.empty:
        product_analysis = product_pages.groupby('pagePath').agg({
            'sessions_page': 'sum'
        }).sort_values('sessions_page', ascending=False)
        journey_analysis['product_performance'] = product_analysis
    
    # コレクションページ分析
    collection_pages = df[df['pagePath'].str.contains('/collections/', na=False)]
    if not collection_pages.empty:
        collection_analysis = collection_pages.groupby('pagePath').agg({
            'sessions_page': 'sum'
        }).sort_values('sessions_page', ascending=False)
        journey_analysis['collection_performance'] = collection_analysis
    
    return journey_analysis

def analyze_conversion_funnel(df):
    """コンバージョンファネル分析"""
    funnel_analysis = {}
    
    # 主要ページのセッション数
    key_pages = {
        'home': df[df['pagePath'] == '/']['sessions_page'].sum(),
        'cart': df[df['pagePath'] == '/cart']['sessions_page'].sum(),
        'checkout': df[df['pagePath'].str.contains('/checkouts/', na=False)]['sessions_page'].sum(),
        'products': df[df['pagePath'].str.contains('/products/', na=False)]['sessions_page'].sum(),
        'collections': df[df['pagePath'].str.contains('/collections/', na=False)]['sessions_page'].sum()
    }
    
    funnel_analysis['page_funnel'] = key_pages
    
    # コンバージョン率計算
    total_sessions = df['sessions'].sum()
    funnel_analysis['conversion_rates'] = {
        'cart_rate': (key_pages['cart'] / total_sessions) * 100,
        'checkout_rate': (key_pages['checkout'] / total_sessions) * 100,
        'product_view_rate': (key_pages['products'] / total_sessions) * 100
    }
    
    return funnel_analysis

def generate_ga4_insights(analysis_results):
    """GA4データから洞察を生成"""
    insights = []
    
    # 基本統計の洞察
    total_sessions = analysis_results['total_sessions']
    total_revenue = analysis_results['total_revenue']
    avg_revenue_per_session = analysis_results['avg_revenue_per_session']
    
    insights.append(f"📊 **総セッション数**: {total_sessions:,}回")
    insights.append(f"💰 **総収益**: ¥{total_revenue:,.0f}")
    insights.append(f"📈 **セッション単価**: ¥{avg_revenue_per_session:,.0f}")
    
    # トラフィックソースの洞察
    traffic_sources = analysis_results['traffic_sources']
    top_source = traffic_sources.index[0]
    top_source_sessions = traffic_sources.iloc[0]['sessions']
    top_source_revenue = traffic_sources.iloc[0]['totalRevenue']
    
    insights.append(f"\n🎯 **主要トラフィックソース**: {top_source}")
    insights.append(f"   - セッション数: {top_source_sessions:,}回")
    insights.append(f"   - 収益: ¥{top_source_revenue:,.0f}")
    
    # コンバージョンファネルの洞察
    if 'conversion_rates' in analysis_results:
        rates = analysis_results['conversion_rates']
        insights.append(f"\n🔄 **コンバージョン率**")
        insights.append(f"   - カート追加率: {rates['cart_rate']:.1f}%")
        insights.append(f"   - チェックアウト率: {rates['checkout_rate']:.1f}%")
        insights.append(f"   - 商品ページ閲覧率: {rates['product_view_rate']:.1f}%")
    
    return insights

def create_ga4_analysis_report():
    """GA4詳細分析レポートを作成"""
    print("🔍 GA4データの詳細分析を開始します...")
    
    # データ読み込み
    df = load_latest_ga4_data()
    if df is None:
        print("❌ GA4データが見つかりません")
        return None
    
    print(f"✅ GA4データ読み込み完了: {len(df)}行")
    
    # 分析実行
    user_behavior = analyze_user_behavior(df)
    customer_journey = analyze_customer_journey(df)
    conversion_funnel = analyze_conversion_funnel(df)
    
    # 洞察生成
    insights = generate_ga4_insights(user_behavior)
    
    # レポート作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"data/reports/ga4_detailed_analysis_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 🔍 GA4詳細分析レポート\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        
        f.write("## 📊 主要指標\n")
        for insight in insights:
            f.write(f"{insight}\n")
        
        f.write("\n## 🎯 トラフィックソース分析\n")
        f.write("| ソース | セッション数 | 収益 | セッション単価 |\n")
        f.write("|--------|-------------|------|----------------|\n")
        for source, data in user_behavior['traffic_sources'].iterrows():
            f.write(f"| {source} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} | ¥{data['revenue_per_session']:,.0f} |\n")
        
        f.write("\n## 📄 人気ページTOP10\n")
        f.write("| ページ | セッション数 |\n")
        f.write("|--------|-------------|\n")
        for page, sessions in user_behavior['top_pages'].head(10).iterrows():
            f.write(f"| {page} | {sessions['sessions_page']:,} |\n")
        
        f.write("\n## 🛒 商品ページ分析\n")
        if 'product_performance' in customer_journey:
            f.write("| 商品ページ | セッション数 |\n")
            f.write("|------------|-------------|\n")
            for page, sessions in customer_journey['product_performance'].head(10).iterrows():
                f.write(f"| {page} | {sessions['sessions_page']:,} |\n")
        
        f.write("\n## 📈 日別トレンド\n")
        f.write("| 日付 | セッション数 | 収益 |\n")
        f.write("|------|-------------|------|\n")
        for date, data in user_behavior['daily_trend'].iterrows():
            f.write(f"| {date} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} |\n")
    
    print(f"✅ GA4詳細分析レポート作成完了: {report_file}")
    return report_file

if __name__ == "__main__":
    create_ga4_analysis_report()

