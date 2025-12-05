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

def correct_ga4_revenue(df):
    """GA4収益の重複を除去して正確な売上を計算"""
    
    # 日付・ソース・セッションでグループ化して重複を除去
    corrected_revenue = df.groupby(['date', 'source', 'sessions']).agg({
        'totalRevenue': 'first',  # 最初の値のみを使用
        'sessions_page': 'sum'    # ページ別セッションは合計
    }).reset_index()
    
    print(f"✅ 重複除去前: {len(df)}行")
    print(f"✅ 重複除去後: {len(corrected_revenue)}行")
    
    return corrected_revenue

def analyze_corrected_revenue(df_corrected):
    """修正された収益データを分析"""
    
    analysis = {}
    
    # 基本統計
    analysis['total_sessions'] = df_corrected['sessions'].sum()
    analysis['total_revenue'] = df_corrected['totalRevenue'].sum()
    analysis['avg_revenue_per_session'] = analysis['total_revenue'] / analysis['total_sessions']
    
    # トラフィックソース別分析
    source_analysis = df_corrected.groupby('source').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_values('sessions', ascending=False)
    
    source_analysis['revenue_per_session'] = source_analysis['totalRevenue'] / source_analysis['sessions']
    analysis['traffic_sources'] = source_analysis
    
    # 日別分析
    daily_analysis = df_corrected.groupby('date').agg({
        'sessions': 'sum',
        'totalRevenue': 'sum'
    }).sort_index()
    analysis['daily_trend'] = daily_analysis
    
    return analysis

def compare_with_shopify_revenue():
    """Shopify売上とGA4収益を比較"""
    
    # Shopifyデータの読み込み
    shopify_files = list(Path("data/raw").glob("shopify_orders_*.csv"))
    if not shopify_files:
        return None
    
    latest_shopify = max(shopify_files, key=lambda x: x.stat().st_mtime)
    shopify_df = pd.read_csv(latest_shopify)
    
    # Shopify売上の計算
    if 'total_price' in shopify_df.columns:
        shopify_df['total_price_num'] = pd.to_numeric(shopify_df['total_price'], errors='coerce').fillna(0.0)
        shopify_revenue = shopify_df['total_price_num'].sum()
    else:
        shopify_revenue = 0
    
    return {
        'shopify_revenue': shopify_revenue,
        'shopify_orders': len(shopify_df)
    }

def create_revenue_comparison_report():
    """収益比較レポートを作成"""
    
    print("🔍 GA4収益の正確性分析を開始します...")
    
    # GA4データ読み込み
    df = load_latest_ga4_data()
    if df is None:
        print("❌ GA4データが見つかりません")
        return None
    
    print(f"✅ GA4データ読み込み完了: {len(df)}行")
    
    # 収益の重複除去
    df_corrected = correct_ga4_revenue(df)
    
    # 修正された収益の分析
    corrected_analysis = analyze_corrected_revenue(df_corrected)
    
    # Shopify売上との比較
    shopify_data = compare_with_shopify_revenue()
    
    # レポート作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"data/reports/ga4_revenue_correction_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 🔍 GA4収益正確性分析レポート\n")
        f.write(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        
        f.write("## 📊 収益比較\n")
        f.write(f"- **GA4収益（重複あり）**: ¥{df['totalRevenue'].sum():,.0f}\n")
        f.write(f"- **GA4収益（重複除去）**: ¥{corrected_analysis['total_revenue']:,.0f}\n")
        
        if shopify_data:
            f.write(f"- **Shopify売上**: ¥{shopify_data['shopify_revenue']:,.0f}\n")
            f.write(f"- **Shopify注文数**: {shopify_data['shopify_orders']}件\n")
        
        f.write(f"- **重複率**: {(df['totalRevenue'].sum() / corrected_analysis['total_revenue'] - 1) * 100:.1f}%\n")
        
        f.write("\n## 🎯 トラフィックソース別正確収益\n")
        f.write("| ソース | セッション数 | 正確収益 | セッション単価 |\n")
        f.write("|--------|-------------|----------|----------------|\n")
        for source, data in corrected_analysis['traffic_sources'].iterrows():
            f.write(f"| {source} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} | ¥{data['revenue_per_session']:,.0f} |\n")
        
        f.write("\n## 📈 日別正確収益\n")
        f.write("| 日付 | セッション数 | 正確収益 |\n")
        f.write("|------|-------------|----------|\n")
        for date, data in corrected_analysis['daily_trend'].iterrows():
            f.write(f"| {date} | {data['sessions']:,} | ¥{data['totalRevenue']:,.0f} |\n")
        
        f.write("\n## 💡 分析結果\n")
        f.write("1. **重複問題**: GA4データでは同じ収益が複数ページに重複して記録されている\n")
        f.write("2. **正確性**: 重複を除去することで、より正確な収益分析が可能\n")
        f.write("3. **比較**: Shopify売上との比較により、データの信頼性を確認\n")
    
    print(f"✅ 収益正確性分析レポート作成完了: {report_file}")
    return report_file

if __name__ == "__main__":
    create_revenue_comparison_report()

