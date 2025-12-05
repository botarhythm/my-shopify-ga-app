import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

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

def analyze_user_behavior(df):
    """ユーザー行動の基本分析"""
    
    # 重複を除去したデータ
    df_unique = df.groupby(['date', 'source', 'sessions']).agg({
        'totalRevenue': 'first',
        'pagePath': lambda x: list(x),
        'sessions_page': 'sum'
    }).reset_index()
    
    behavior_analysis = {}
    
    # 基本統計
    behavior_analysis['total_sessions'] = df['sessions'].sum()
    behavior_analysis['total_revenue'] = df_unique['totalRevenue'].sum()
    behavior_analysis['avg_revenue_per_session'] = behavior_analysis['total_revenue'] / behavior_analysis['total_sessions'] if behavior_analysis['total_sessions'] > 0 else 0
    
    # トラフィックソース分析
    source_analysis = df_unique.groupby('source').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_values('sessions', ascending=False)
    
    source_analysis['revenue_per_session'] = source_analysis['totalRevenue'] / source_analysis['sessions']
    behavior_analysis['traffic_sources'] = source_analysis
    
    # 人気ページ分析
    all_pages = []
    for pages in df_unique['pagePath']:
        all_pages.extend(pages)
    
    page_frequency = pd.Series(all_pages).value_counts()
    behavior_analysis['top_pages'] = page_frequency.head(10)
    
    # 商品ページ分析
    product_pages = df[df['pagePath'].str.contains('/products/', na=False)]
    if not product_pages.empty:
        product_analysis = product_pages.groupby('pagePath').agg({
            'sessions_page': 'sum'
        }).sort_values('sessions_page', ascending=False).head(10)
        behavior_analysis['product_performance'] = product_analysis
    
    # 日別トレンド
    daily_trend = df_unique.groupby('date').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_index()
    behavior_analysis['daily_trend'] = daily_trend
    
    return behavior_analysis

def analyze_customer_journey(df):
    """顧客ジャーニー分析"""
    
    journey_analysis = {}
    
    # コンバージョンファネル分析
    funnel_stages = {
        'home': df[df['pagePath'] == '/']['sessions_page'].sum(),
        'collections': df[df['pagePath'].str.contains('/collections/', na=False)]['sessions_page'].sum(),
        'products': df[df['pagePath'].str.contains('/products/', na=False)]['sessions_page'].sum(),
        'cart': df[df['pagePath'] == '/cart']['sessions_page'].sum(),
        'checkout': df[df['pagePath'].str.contains('/checkouts/', na=False)]['sessions_page'].sum()
    }
    
    total_sessions = df['sessions'].sum()
    conversion_rates = {}
    for stage, sessions in funnel_stages.items():
        conversion_rates[stage] = (sessions / total_sessions) * 100 if total_sessions > 0 else 0
    
    journey_analysis['funnel'] = funnel_stages
    journey_analysis['conversion_rates'] = conversion_rates
    
    return journey_analysis

def analyze_traffic_source_effectiveness(df):
    """トラフィックソースの効果分析"""
    
    # 重複を除去
    df_unique = df.groupby(['date', 'source', 'sessions']).agg({
        'totalRevenue': 'first'
    }).reset_index()
    
    source_analysis = df_unique.groupby('source').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_values('sessions', ascending=False)
    
    source_analysis['revenue_per_session'] = source_analysis['totalRevenue'] / source_analysis['sessions']
    source_analysis['conversion_rate'] = (source_analysis['totalRevenue'] > 0).astype(int) * 100
    
    # ソース別の効果性評価
    source_analysis['effectiveness_score'] = (
        source_analysis['revenue_per_session'] * 
        source_analysis['conversion_rate'] / 100
    )
    
    return source_analysis

def analyze_temporal_patterns(df):
    """時間的パターンの分析"""
    
    # 重複を除去
    df_unique = df.groupby(['date', 'source', 'sessions']).agg({
        'totalRevenue': 'first'
    }).reset_index()
    
    temporal_analysis = {}
    
    # 日別パターン
    daily_pattern = df_unique.groupby('date').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_index()
    
    # 曜日の追加
    daily_pattern.index = pd.to_datetime(daily_pattern.index)
    daily_pattern['weekday'] = daily_pattern.index.day_name()
    daily_pattern['weekday_num'] = daily_pattern.index.dayofweek
    
    temporal_analysis['daily'] = daily_pattern
    
    # 曜日別分析
    weekday_analysis = daily_pattern.groupby('weekday').agg({
        'sessions': 'mean',
        'totalRevenue': 'mean'
    }).reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    
    temporal_analysis['weekday'] = weekday_analysis
    
    return temporal_analysis

def analyze_user_behavior_segments(df):
    """ユーザー行動セグメント分析"""
    
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

def generate_strategic_insights(df):
    """戦略的洞察の生成"""
    
    insights = []
    
    # 1. 顧客ジャーニー分析
    journey = analyze_customer_journey(df)
    rates = journey['conversion_rates']
    
    insights.append("## 🎯 顧客ジャーニー戦略")
    insights.append(f"- **ホームページ離脱率**: {100 - rates['home']:.1f}%")
    insights.append(f"- **商品ページ到達率**: {rates['products']:.1f}%")
    insights.append(f"- **カート追加率**: {rates['cart']:.1f}%")
    insights.append(f"- **チェックアウト開始率**: {rates['checkout']:.1f}%")
    
    # 2. トラフィックソース戦略
    source_analysis = analyze_traffic_source_effectiveness(df)
    
    insights.append("\n## 📈 トラフィックソース戦略")
    top_source = source_analysis.iloc[0]
    insights.append(f"- **最有力ソース**: {source_analysis.index[0]} (セッション単価: ¥{top_source['revenue_per_session']:,.0f})")
    
    # 高ROIソースの特定
    high_roi_sources = source_analysis[source_analysis['revenue_per_session'] > 1000]
    if not high_roi_sources.empty:
        insights.append("- **高ROIソース**:")
        for source in high_roi_sources.index[:3]:
            roi = high_roi_sources.loc[source, 'revenue_per_session']
            insights.append(f"  - {source}: ¥{roi:,.0f}/セッション")
    
    # 3. 時間戦略
    temporal = analyze_temporal_patterns(df)
    
    insights.append("\n## ⏰ 時間戦略")
    weekday_analysis = temporal['weekday']
    best_day = weekday_analysis['totalRevenue'].idxmax()
    best_revenue = weekday_analysis.loc[best_day, 'totalRevenue']
    insights.append(f"- **最売上曜日**: {best_day} (平均¥{best_revenue:,.0f})")
    
    # 4. ユーザーセグメント戦略
    segments = analyze_user_behavior_segments(df)
    
    insights.append("\n## 👥 ユーザーセグメント戦略")
    best_segment = segments['avg_revenue'].idxmax()
    best_avg_revenue = segments.loc[best_segment, 'avg_revenue']
    insights.append(f"- **最価値セグメント**: {best_segment} (平均¥{best_avg_revenue:,.0f})")
    
    return insights

def create_unified_ga4_report():
    """統合GA4分析レポートを作成"""
    
    print("🔍 GA4統合分析を開始します...")
    
    # データ読み込み
    df = load_latest_ga4_data()
    if df is None:
        print("❌ GA4データが見つかりません")
        return None
    
    print(f"✅ GA4データ読み込み完了: {len(df)}行")
    
    # 分析実行
    user_behavior = analyze_user_behavior(df)
    customer_journey = analyze_customer_journey(df)
    source_analysis = analyze_traffic_source_effectiveness(df)
    temporal_analysis = analyze_temporal_patterns(df)
    segment_analysis = analyze_user_behavior_segments(df)
    strategic_insights = generate_strategic_insights(df)
    
    # レポート作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"data/reports/ga4_unified_analysis_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 🔍 GA4統合分析レポート\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        
        # 主要指標
        f.write("## 📊 主要指標\n")
        f.write(f"📊 **総セッション数**: {user_behavior['total_sessions']:,}回\n")
        f.write(f"💰 **総収益**: ¥{user_behavior['total_revenue']:,.0f}\n")
        f.write(f"📈 **セッション単価**: ¥{user_behavior['avg_revenue_per_session']:,.0f}\n\n")
        
        # 戦略的洞察
        f.write("## 🎯 戦略的洞察\n")
        for insight in strategic_insights:
            f.write(f"{insight}\n")
        
        f.write("\n## 📊 詳細分析データ\n")
        
        # コンバージョンファネル
        f.write("\n### 🔄 コンバージョンファネル\n")
        f.write("| ステージ | セッション数 | コンバージョン率 |\n")
        f.write("|----------|-------------|------------------|\n")
        for stage, sessions in customer_journey['funnel'].items():
            rate = customer_journey['conversion_rates'][stage]
            f.write(f"| {stage} | {sessions:,} | {rate:.1f}% |\n")
        
        # トラフィックソース効果性
        f.write("\n### 🎯 トラフィックソース効果性\n")
        f.write("| ソース | セッション数 | 収益 | セッション単価 | 効果性スコア |\n")
        f.write("|--------|-------------|------|----------------|--------------|\n")
        for source, data in source_analysis.head(10).iterrows():
            f.write(f"| {source} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} | ¥{data['revenue_per_session']:,.0f} | {data['effectiveness_score']:.1f} |\n")
        
        # 人気ページTOP10
        f.write("\n### 📄 人気ページTOP10\n")
        f.write("| ページ | セッション数 |\n")
        f.write("|--------|-------------|\n")
        for page, sessions in user_behavior['top_pages'].items():
            f.write(f"| {page} | {sessions:,} |\n")
        
        # 商品ページ分析
        if 'product_performance' in user_behavior:
            f.write("\n### 🛒 商品ページ分析\n")
            f.write("| 商品ページ | セッション数 |\n")
            f.write("|------------|-------------|\n")
            for page, data in user_behavior['product_performance'].iterrows():
                f.write(f"| {page} | {data['sessions_page']:,} |\n")
        
        # 曜日別パフォーマンス
        f.write("\n### ⏰ 曜日別パフォーマンス\n")
        f.write("| 曜日 | 平均セッション数 | 平均収益 |\n")
        f.write("|------|------------------|----------|\n")
        for weekday, data in temporal_analysis['weekday'].iterrows():
            f.write(f"| {weekday} | {data['sessions']:.1f} | ¥{data['totalRevenue']:,.0f} |\n")
        
        # ユーザーセグメント
        f.write("\n### 👥 ユーザーセグメント分析\n")
        f.write("| セグメント | セッション数 | 総収益 | 平均収益 |\n")
        f.write("|------------|-------------|--------|----------|\n")
        for segment, data in segment_analysis.iterrows():
            f.write(f"| {segment} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} | ¥{data['avg_revenue']:,.0f} |\n")
        
        # 日別トレンド
        f.write("\n### 📈 日別トレンド\n")
        f.write("| 日付 | セッション数 | 収益 |\n")
        f.write("|------|-------------|------|\n")
        for date, data in user_behavior['daily_trend'].iterrows():
            f.write(f"| {date} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} |\n")
        
        f.write("\n## 💡 戦略的推奨事項\n")
        f.write("1. **コンバージョン最適化**: ファネルの各段階での離脱率を改善\n")
        f.write("2. **トラフィック戦略**: 高ROIソースへの投資拡大\n")
        f.write("3. **商品戦略**: 人気商品の在庫確保とプロモーション強化\n")
        f.write("4. **時間戦略**: 最売上曜日のマーケティング強化\n")
        f.write("5. **セグメント戦略**: 高価値ユーザーセグメントへのターゲティング\n")
    
    print(f"✅ 統合GA4分析レポート作成完了: {report_file}")
    return report_file

if __name__ == "__main__":
    create_unified_ga4_report()

