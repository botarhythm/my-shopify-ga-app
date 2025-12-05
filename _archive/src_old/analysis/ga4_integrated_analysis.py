import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def load_latest_ga4_data():
    """最新のGA4データを読み込み"""
    data_dir = Path("data/raw")
    ga4_files = list(data_dir.glob("ga4_data_*.csv"))
    
    if not ga4_files:
        return None
    
    latest_file = max(ga4_files, key=lambda x: x.stat().st_mtime)
    print(f"📊 GA4データファイルを読み込み中: {latest_file.name}")
    
    df = pd.read_csv(latest_file)
    return df

def analyze_traffic_patterns(df):
    """トラフィックパターンの分析（信頼性の高いデータ）"""
    
    # 重複を除去
    df_unique = df.groupby(['date', 'source', 'sessions']).agg({
        'totalRevenue': 'first'
    }).reset_index()
    
    traffic_analysis = {}
    
    # 1. トラフィックソース分析
    source_analysis = df_unique.groupby('source').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_values('sessions', ascending=False)
    
    source_analysis['revenue_per_session'] = source_analysis['totalRevenue'] / source_analysis['sessions']
    traffic_analysis['sources'] = source_analysis
    
    # 2. 日別トラフィックトレンド
    daily_trend = df_unique.groupby('date').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_index()
    
    daily_trend.index = pd.to_datetime(daily_trend.index)
    daily_trend['weekday'] = daily_trend.index.day_name()
    daily_trend['weekday_num'] = daily_trend.index.dayofweek
    
    traffic_analysis['daily_trend'] = daily_trend
    
    # 3. 曜日別分析
    weekday_analysis = daily_trend.groupby('weekday').agg({
        'sessions': 'mean',
        'totalRevenue': 'mean'
    }).reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    
    traffic_analysis['weekday'] = weekday_analysis
    
    return traffic_analysis

def analyze_user_behavior(df):
    """ユーザー行動分析（ページ訪問パターン）"""
    
    behavior_analysis = {}
    
    # 1. ページ別訪問数
    all_pages = []
    for pages in df['pagePath'].dropna():
        if isinstance(pages, str):
            all_pages.append(pages)
        elif isinstance(pages, list):
            all_pages.extend(pages)
    
    page_visits = pd.Series(all_pages).value_counts()
    behavior_analysis['page_popularity'] = page_visits.head(20)
    
    # 2. 商品ページ分析
    product_pages = df[df['pagePath'].str.contains('/products/', na=False)]
    if not product_pages.empty:
        product_analysis = product_pages.groupby('pagePath').agg({
            'sessions_page': 'sum'
        }).sort_values('sessions_page', ascending=False)
        
        # 商品名の抽出
        product_analysis['product_name'] = product_analysis.index.str.extract(r'/products/([^/]+)')[0]
        product_analysis = product_analysis.dropna(subset=['product_name'])
        
        behavior_analysis['product_performance'] = product_analysis
    
    # 3. コレクションページ分析
    collection_pages = df[df['pagePath'].str.contains('/collections/', na=False)]
    if not collection_pages.empty:
        collection_analysis = collection_pages.groupby('pagePath').agg({
            'sessions_page': 'sum'
        }).sort_values('sessions_page', ascending=False)
        behavior_analysis['collection_performance'] = collection_analysis
    
    return behavior_analysis

def analyze_conversion_funnel(df):
    """コンバージョンファネル分析（セッション数ベース）"""
    
    funnel_analysis = {}
    
    # 主要ページのセッション数
    key_pages = {
        'home': df[df['pagePath'] == '/']['sessions_page'].sum(),
        'collections': df[df['pagePath'].str.contains('/collections/', na=False)]['sessions_page'].sum(),
        'products': df[df['pagePath'].str.contains('/products/', na=False)]['sessions_page'].sum(),
        'cart': df[df['pagePath'] == '/cart']['sessions_page'].sum(),
        'checkout': df[df['pagePath'].str.contains('/checkouts/', na=False)]['sessions_page'].sum()
    }
    
    funnel_analysis['page_funnel'] = key_pages
    
    # コンバージョン率計算（セッション数ベース）
    total_sessions = df['sessions'].sum()
    funnel_analysis['conversion_rates'] = {
        'home_rate': (key_pages['home'] / total_sessions) * 100 if total_sessions > 0 else 0,
        'collections_rate': (key_pages['collections'] / total_sessions) * 100 if total_sessions > 0 else 0,
        'products_rate': (key_pages['products'] / total_sessions) * 100 if total_sessions > 0 else 0,
        'cart_rate': (key_pages['cart'] / total_sessions) * 100 if total_sessions > 0 else 0,
        'checkout_rate': (key_pages['checkout'] / total_sessions) * 100 if total_sessions > 0 else 0
    }
    
    return funnel_analysis

def analyze_user_segments(df):
    """ユーザーセグメント分析（行動パターンベース）"""
    
    # 重複を除去
    df_unique = df.groupby(['date', 'source', 'sessions']).agg({
        'totalRevenue': 'first',
        'pagePath': lambda x: len(set(x))  # 訪問ページ数
    }).reset_index()
    
    # ユーザーセグメントの定義
    def segment_users(row):
        if row['totalRevenue'] > 0:
            if row['pagePath'] >= 5:
                return '高価値・多ページ訪問'
            else:
                return '高価値・少ページ訪問'
        else:
            if row['pagePath'] >= 5:
                return '低価値・多ページ訪問'
            else:
                return '低価値・少ページ訪問'
    
    df_unique['user_segment'] = df_unique.apply(segment_users, axis=1)
    
    segment_analysis = df_unique.groupby('user_segment').agg({
        'sessions': 'count',
        'totalRevenue': 'sum',
        'pagePath': 'mean'
    })
    
    segment_analysis['avg_revenue'] = segment_analysis['totalRevenue'] / segment_analysis['sessions']
    
    return segment_analysis

def create_visualizations(traffic_analysis, behavior_analysis, funnel_analysis, segment_analysis):
    """分析結果の可視化"""
    
    charts = {}
    
    # 1. トラフィックソース円グラフ
    if not traffic_analysis['sources'].empty:
        fig_sources = px.pie(
            traffic_analysis['sources'].head(10), 
            values='sessions', 
            names=traffic_analysis['sources'].head(10).index,
            title='トラフィックソース分布'
        )
        charts['traffic_sources'] = fig_sources
    
    # 2. 日別セッション数トレンド
    if not traffic_analysis['daily_trend'].empty:
        fig_trend = px.line(
            traffic_analysis['daily_trend'], 
            y='sessions',
            title='日別セッション数トレンド'
        )
        charts['daily_trend'] = fig_trend
    
    # 3. 曜日別パフォーマンス
    if not traffic_analysis['weekday'].empty:
        fig_weekday = px.bar(
            traffic_analysis['weekday'], 
            y='sessions',
            title='曜日別平均セッション数'
        )
        charts['weekday'] = fig_weekday
    
    # 4. コンバージョンファネル
    funnel_data = pd.DataFrame({
        'ステージ': list(funnel_analysis['page_funnel'].keys()),
        'セッション数': list(funnel_analysis['page_funnel'].values())
    })
    
    fig_funnel = px.funnel(
        funnel_data, 
        x='セッション数', 
        y='ステージ',
        title='コンバージョンファネル'
    )
    charts['funnel'] = fig_funnel
    
    # 5. 人気ページTOP10
    if not behavior_analysis['page_popularity'].empty:
        top_pages = behavior_analysis['page_popularity'].head(10)
        fig_pages = px.bar(
            x=top_pages.values, 
            y=top_pages.index,
            orientation='h',
            title='人気ページTOP10'
        )
        charts['popular_pages'] = fig_pages
    
    return charts

def generate_insights(traffic_analysis, behavior_analysis, funnel_analysis, segment_analysis):
    """洞察の生成"""
    
    insights = []
    
    # 1. トラフィック洞察
    total_sessions = traffic_analysis['sources']['sessions'].sum()
    insights.append(f"📊 **総セッション数**: {total_sessions:,}回")
    
    top_source = traffic_analysis['sources'].iloc[0]
    insights.append(f"🎯 **主要トラフィックソース**: {traffic_analysis['sources'].index[0]} ({top_source['sessions']:,}セッション)")
    
    # 2. コンバージョンファネル洞察
    rates = funnel_analysis['conversion_rates']
    insights.append(f"\n🔄 **コンバージョンファネル**")
    insights.append(f"- ホームページ到達率: {rates['home_rate']:.1f}%")
    insights.append(f"- 商品ページ到達率: {rates['products_rate']:.1f}%")
    insights.append(f"- カート追加率: {rates['cart_rate']:.1f}%")
    insights.append(f"- チェックアウト開始率: {rates['checkout_rate']:.1f}%")
    
    # 3. 曜日別洞察
    best_day = traffic_analysis['weekday']['sessions'].idxmax()
    best_sessions = traffic_analysis['weekday'].loc[best_day, 'sessions']
    insights.append(f"\n⏰ **最アクティブ曜日**: {best_day} (平均{best_sessions:.1f}セッション)")
    
    # 4. ユーザーセグメント洞察
    if not segment_analysis.empty:
        best_segment = segment_analysis['avg_revenue'].idxmax()
        best_avg_revenue = segment_analysis.loc[best_segment, 'avg_revenue']
        insights.append(f"\n👥 **最価値セグメント**: {best_segment} (平均¥{best_avg_revenue:,.0f})")
    
    return insights

def create_integrated_ga4_report():
    """統合GA4分析レポートを作成"""
    
    print("🔍 GA4データの統合分析を開始します...")
    
    # データ読み込み
    df = load_latest_ga4_data()
    if df is None:
        print("❌ GA4データが見つかりません")
        return None
    
    print(f"✅ GA4データ読み込み完了: {len(df)}行")
    
    # 分析実行
    traffic_analysis = analyze_traffic_patterns(df)
    behavior_analysis = analyze_user_behavior(df)
    funnel_analysis = analyze_conversion_funnel(df)
    segment_analysis = analyze_user_segments(df)
    
    # 可視化作成
    charts = create_visualizations(traffic_analysis, behavior_analysis, funnel_analysis, segment_analysis)
    
    # 洞察生成
    insights = generate_insights(traffic_analysis, behavior_analysis, funnel_analysis, segment_analysis)
    
    # レポート作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"data/reports/ga4_integrated_analysis_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 🔍 GA4統合分析レポート\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        f.write("> **注意**: このレポートはGA4から取得可能な信頼性の高いデータのみを分析対象としています。\n")
        f.write("> 売上データは実際の数値と異なる可能性があるため、トラフィックとユーザー行動に焦点を当てています。\n\n")
        
        # 主要洞察
        f.write("## 💡 主要洞察\n")
        for insight in insights:
            f.write(f"{insight}\n")
        
        # トラフィックソース分析
        f.write("\n## 🎯 トラフィックソース分析\n")
        f.write("| ソース | セッション数 | 収益 | セッション単価 |\n")
        f.write("|--------|-------------|------|----------------|\n")
        for source, data in traffic_analysis['sources'].head(10).iterrows():
            f.write(f"| {source} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} | ¥{data['revenue_per_session']:,.0f} |\n")
        
        # コンバージョンファネル
        f.write("\n## 🔄 コンバージョンファネル\n")
        f.write("| ステージ | セッション数 | コンバージョン率 |\n")
        f.write("|----------|-------------|------------------|\n")
        for stage, sessions in funnel_analysis['page_funnel'].items():
            rate = funnel_analysis['conversion_rates'][f'{stage}_rate']
            f.write(f"| {stage} | {sessions:,} | {rate:.1f}% |\n")
        
        # 人気ページ
        f.write("\n## 📄 人気ページTOP10\n")
        f.write("| ページ | セッション数 |\n")
        f.write("|--------|-------------|\n")
        for page, sessions in behavior_analysis['page_popularity'].head(10).items():
            f.write(f"| {page} | {sessions:,} |\n")
        
        # 商品ページ分析
        if 'product_performance' in behavior_analysis:
            f.write("\n## 🛒 商品ページ分析TOP10\n")
            f.write("| 商品名 | セッション数 |\n")
            f.write("|--------|-------------|\n")
            for page, data in behavior_analysis['product_performance'].head(10).iterrows():
                f.write(f"| {data['product_name']} | {data['sessions_page']:,} |\n")
        
        # 曜日別パフォーマンス
        f.write("\n## ⏰ 曜日別パフォーマンス\n")
        f.write("| 曜日 | 平均セッション数 | 平均収益 |\n")
        f.write("|------|------------------|----------|\n")
        for weekday, data in traffic_analysis['weekday'].iterrows():
            f.write(f"| {weekday} | {data['sessions']:.1f} | ¥{data['totalRevenue']:,.0f} |\n")
        
        # ユーザーセグメント
        if not segment_analysis.empty:
            f.write("\n## 👥 ユーザーセグメント分析\n")
            f.write("| セグメント | セッション数 | 総収益 | 平均収益 |\n")
            f.write("|------------|-------------|--------|----------|\n")
            for segment, data in segment_analysis.iterrows():
                f.write(f"| {segment} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} | ¥{data['avg_revenue']:,.0f} |\n")
        
        f.write("\n## 💡 戦略的推奨事項\n")
        f.write("1. **コンバージョン最適化**: ファネルの各段階での離脱率を改善\n")
        f.write("2. **トラフィック戦略**: 高セッション単価ソースへの投資拡大\n")
        f.write("3. **コンテンツ戦略**: 人気ページのコンテンツ強化\n")
        f.write("4. **時間戦略**: 最アクティブ曜日のマーケティング強化\n")
        f.write("5. **セグメント戦略**: 高価値ユーザーセグメントへのターゲティング\n")
    
    print(f"✅ GA4統合分析レポート作成完了: {report_file}")
    return report_file

if __name__ == "__main__":
    create_integrated_ga4_report()

