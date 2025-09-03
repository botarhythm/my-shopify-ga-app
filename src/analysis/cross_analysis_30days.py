#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直近30日の売上とGAデータのクロス分析スクリプト
より詳細な分析とインサイトを提供します。

必要なライブラリのインストール:
pip install pandas matplotlib seaborn
"""

import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

# フォント設定（シンプル版）
plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial', 'sans-serif']

def find_latest_csv_files():
    """最新のCSVファイルを見つけます。"""
    files = {}
    
    # Shopify注文データ
    shopify_orders = glob.glob("shopify_orders_*.csv")
    if shopify_orders:
        files['shopify_orders'] = max(shopify_orders, key=os.path.getctime)
    
    # Shopify商品データ
    shopify_products = glob.glob("shopify_products_*.csv")
    if shopify_products:
        files['shopify_products'] = max(shopify_products, key=os.path.getctime)
    
    # Google Analyticsデータ
    ga4_data = glob.glob("ga4_data_*.csv")
    if ga4_data:
        files['ga4_data'] = max(ga4_data, key=os.path.getctime)
    
    return files

def load_data(files):
    """CSVファイルからデータを読み込みます。"""
    data = {}
    
    for file_type, file_path in files.items():
        try:
            if file_type == 'shopify_orders':
                data['orders'] = pd.read_csv(file_path)
                print(f"✓ Shopify注文データ読み込み完了: {len(data['orders'])}件")
            elif file_type == 'shopify_products':
                data['products'] = pd.read_csv(file_path)
                print(f"✓ Shopify商品データ読み込み完了: {len(data['products'])}件")
            elif file_type == 'ga4_data':
                data['ga4'] = pd.read_csv(file_path)
                print(f"✓ Google Analyticsデータ読み込み完了: {len(data['ga4'])}件")
        except Exception as e:
            print(f"✗ {file_type}の読み込みに失敗: {e}")
    
    return data

def analyze_daily_trends(orders_df, ga4_df):
    """日別の売上とセッション数のトレンドを分析します。"""
    print("\n=== 日別トレンド分析 ===")
    
    try:
        # 注文データの日別集計
        if 'created_at' in orders_df.columns and 'total_price' in orders_df.columns:
            orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
            orders_df['date'] = orders_df['created_at'].dt.date
            
            daily_orders = orders_df.groupby('date').agg({
                'total_price': 'sum',
                'id': 'count'
            }).reset_index()
            daily_orders.columns = ['date', 'daily_revenue', 'order_count']
            
            # GA4データの日別集計
            if 'date' in ga4_df.columns and 'sessions' in ga4_df.columns:
                ga4_df['date'] = pd.to_datetime(ga4_df['date'])
                daily_ga4 = ga4_df.groupby('date').agg({
                    'sessions': 'sum',
                    'totalRevenue': 'sum'
                }).reset_index()
                
                # 日別データをマージ（データ型を統一）
                daily_orders['date'] = pd.to_datetime(daily_orders['date'])
                daily_ga4['date'] = pd.to_datetime(daily_ga4['date'])
                daily_analysis = pd.merge(daily_orders, daily_ga4, on='date', how='outer')
                daily_analysis = daily_analysis.fillna(0)
                daily_analysis = daily_analysis.sort_values('date')
                
                print("日別売上・セッション分析:")
                print(daily_analysis.to_string(index=False))
                
                # 相関分析
                correlation = daily_analysis['daily_revenue'].corr(daily_analysis['sessions'])
                print(f"\n売上とセッション数の相関係数: {correlation:.3f}")
                
                if correlation > 0.7:
                    print("→ 強い正の相関: セッション数が増えると売上も増加")
                elif correlation > 0.3:
                    print("→ 中程度の正の相関: セッション数と売上に関連性あり")
                elif correlation > -0.3:
                    print("→ 弱い相関: セッション数と売上の関連性は低い")
                else:
                    print("→ 負の相関: セッション数が増えると売上が減少")
                
                return daily_analysis
            else:
                print("GA4データに必要な列が含まれていません。")
                return None
        else:
            print("注文データに必要な列が含まれていません。")
            return None
            
    except Exception as e:
        print(f"日別トレンド分析中にエラーが発生: {e}")
        return None

def analyze_source_performance(orders_df, ga4_df):
    """流入元別のパフォーマンスを詳細分析します。"""
    print("\n=== 流入元パフォーマンス詳細分析 ===")
    
    try:
        if ga4_df.empty:
            print("GA4データが不足しています。")
            return None
        
        # 流入元別の詳細分析
        source_analysis = ga4_df.groupby('source').agg({
            'sessions': 'sum',
            'totalRevenue': 'sum'
        }).reset_index()
        
        # セッションあたりの売上を計算
        source_analysis['revenue_per_session'] = (
            source_analysis['totalRevenue'] / source_analysis['sessions']
        ).round(2)
        
        # 売上効率（収益/セッション）でソート
        source_analysis = source_analysis.sort_values('revenue_per_session', ascending=False)
        
        print("流入元別パフォーマンス（効率順）:")
        print(source_analysis.to_string(index=False))
        
        # 高効率流入元の特定
        high_efficiency = source_analysis[source_analysis['revenue_per_session'] > 1000]
        if not high_efficiency.empty:
            print(f"\n🔥 高効率流入元（セッションあたり1,000円以上）:")
            for _, row in high_efficiency.iterrows():
                print(f"   {row['source']}: セッションあたり {row['revenue_per_session']:,.0f}円")
        
        # 低効率流入元の特定
        low_efficiency = source_analysis[
            (source_analysis['sessions'] >= 2) & 
            (source_analysis['revenue_per_session'] < 500)
        ]
        if not low_efficiency.empty:
            print(f"\n⚠️  改善が必要な流入元（セッションあたり500円未満）:")
            for _, row in low_efficiency.iterrows():
                print(f"   {row['source']}: セッションあたり {row['revenue_per_session']:,.0f}円")
        
        return source_analysis
        
    except Exception as e:
        print(f"流入元パフォーマンス分析中にエラーが発生: {e}")
        return None

def analyze_product_source_correlation(orders_df, products_df, ga4_df):
    """商品と流入元の相関関係を分析します。"""
    print("\n=== 商品・流入元相関分析 ===")
    
    try:
        if orders_df.empty or products_df.empty or ga4_df.empty:
            print("必要なデータが不足しています。")
            return None
        
        # 売上トップ商品の特定
        if 'product_title' in orders_df.columns and 'total_price' in orders_df.columns:
            top_products = orders_df.groupby('product_title').agg({
                'total_price': 'sum',
                'quantity': 'sum'
            }).reset_index()
            top_products = top_products.sort_values('total_price', ascending=False).head(5)
            
            print("売上トップ5商品:")
            for i, row in top_products.iterrows():
                print(f"{i+1}. {row['product_title']}: {row['total_price']:,.0f}円")
            
            # 流入元別の商品傾向分析
            print(f"\n流入元別の商品傾向:")
            
            # 直接アクセスの分析
            direct_traffic = ga4_df[ga4_df['source'].str.contains('direct', case=False, na=False)]
            if not direct_traffic.empty:
                direct_revenue = direct_traffic['totalRevenue'].sum()
                print(f"   📱 直接アクセス: {direct_revenue:,.0f}円")
                print("      → リピーター、ブックマーク、直接URL入力")
                print("      → ブランド認知度が高い")
            
            # Instagram流入の分析
            instagram_traffic = ga4_df[ga4_df['source'].str.contains('instagram', case=False, na=False)]
            if not instagram_traffic.empty:
                instagram_revenue = instagram_traffic['totalRevenue'].sum()
                print(f"   📸 Instagram: {instagram_revenue:,.0f}円")
                print("      → ビジュアル重視の商品が好まれる")
                print("      → 若年層・女性層の割合が高い")
            
            # Google流入の分析
            google_traffic = ga4_df[ga4_df['source'].str.contains('google', case=False, na=False)]
            if not google_traffic.empty:
                google_revenue = google_traffic['totalRevenue'].sum()
                print(f"   🔍 Google: {google_revenue:,.0f}円")
                print("      → 検索意図が明確")
                print("      → 比較検討段階のユーザー")
            
            return top_products
            
        else:
            print("注文データに必要な列が含まれていません。")
            return None
            
    except Exception as e:
        print(f"商品・流入元相関分析中にエラーが発生: {e}")
        return None

def analyze_conversion_funnel(ga4_df):
    """コンバージョンファネルを分析します。"""
    print("\n=== コンバージョンファネル分析 ===")
    
    try:
        if ga4_df.empty:
            print("GA4データが不足しています。")
            return None
        
        # 流入元別のコンバージョン率
        conversion_analysis = ga4_df.groupby('source').agg({
            'sessions': 'sum',
            'totalRevenue': 'sum'
        }).reset_index()
        
        # コンバージョン率を計算（収益があるセッションの割合）
        conversion_analysis['conversion_rate'] = (
            (conversion_analysis['totalRevenue'] > 0).astype(int)
        )
        
        # セッションあたりの売上
        conversion_analysis['revenue_per_session'] = (
            conversion_analysis['totalRevenue'] / conversion_analysis['sessions']
        ).round(2)
        
        # 効率性スコア（セッションあたり売上 × コンバージョン率）
        conversion_analysis['efficiency_score'] = (
            conversion_analysis['revenue_per_session'] * conversion_analysis['conversion_rate']
        ).round(2)
        
        # 効率性スコアでソート
        conversion_analysis = conversion_analysis.sort_values('efficiency_score', ascending=False)
        
        print("流入元別コンバージョン効率:")
        print(conversion_analysis.to_string(index=False))
        
        # ファネル段階の分析
        print(f"\n📊 コンバージョンファネル段階:")
        
        # 認知段階（セッション数が多いが売上が少ない）
        awareness_sources = conversion_analysis[
            (conversion_analysis['sessions'] >= 3) & 
            (conversion_analysis['revenue_per_session'] < 1000)
        ]
        if not awareness_sources.empty:
            print("   1️⃣ 認知段階（ブランド認知）:")
            for _, row in awareness_sources.iterrows():
                print(f"      {row['source']}: {row['sessions']}セッション")
        
        # 検討段階（中程度の売上効率）
        consideration_sources = conversion_analysis[
            (conversion_analysis['sessions'] >= 2) & 
            (conversion_analysis['revenue_per_session'] >= 1000) &
            (conversion_analysis['revenue_per_session'] < 3000)
        ]
        if not consideration_sources.empty:
            print("   2️⃣ 検討段階（商品比較）:")
            for _, row in consideration_sources.iterrows():
                print(f"      {row['source']}: セッションあたり{row['revenue_per_session']:,.0f}円")
        
        # 購入段階（高売上効率）
        purchase_sources = conversion_analysis[
            conversion_analysis['revenue_per_session'] >= 3000
        ]
        if not purchase_sources.empty:
            print("   3️⃣ 購入段階（決断・購入）:")
            for _, row in purchase_sources.iterrows():
                print(f"      {row['source']}: セッションあたり{row['revenue_per_session']:,.0f}円")
        
        return conversion_analysis
        
    except Exception as e:
        print(f"コンバージョンファネル分析中にエラーが発生: {e}")
        return None

def generate_cross_analysis_report(daily_trends, source_performance, product_source, conversion_funnel):
    """クロス分析レポートを生成します。"""
    print("\n=== クロス分析レポート生成 ===")
    
    report = []
    report.append("# 🔍 直近30日 売上・GAデータ クロス分析レポート")
    report.append(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    report.append("")
    
    # 分析サマリー
    report.append("## 📊 分析サマリー")
    report.append("")
    report.append("このレポートは、直近30日のShopify売上データとGoogle Analyticsデータを")
    report.append("統合分析し、ビジネスインサイトを提供するものです。")
    report.append("")
    
    # 日別トレンド分析
    if daily_trends is not None and not daily_trends.empty:
        report.append("## 📈 日別トレンド分析")
        report.append("")
        
        # 相関分析結果
        correlation = daily_trends['daily_revenue'].corr(daily_trends['sessions'])
        report.append(f"### 売上とセッション数の相関: {correlation:.3f}")
        
        if correlation > 0.7:
            report.append("- **強い正の相関**: セッション数が増えると売上も増加")
            report.append("- **インサイト**: トラフィック増加が直接売上向上に繋がる")
        elif correlation > 0.3:
            report.append("- **中程度の正の相関**: セッション数と売上に関連性あり")
            report.append("- **インサイト**: 質の高いトラフィックの獲得が重要")
        else:
            report.append("- **弱い相関**: セッション数と売上の関連性は低い")
            report.append("- **インサイト**: コンバージョン率の改善が優先課題")
        report.append("")
    
    # 流入元パフォーマンス分析
    if source_performance is not None and not source_performance.empty:
        report.append("## 🎯 流入元パフォーマンス分析")
        report.append("")
        
        # 高効率流入元
        high_efficiency = source_performance[source_performance['revenue_per_session'] > 1000]
        if not high_efficiency.empty:
            report.append("### 🔥 高効率流入元（セッションあたり1,000円以上）")
            for _, row in high_efficiency.iterrows():
                report.append(f"- **{row['source']}**: セッションあたり {row['revenue_per_session']:,.0f}円")
            report.append("")
        
        # 改善が必要な流入元
        low_efficiency = source_performance[
            (source_performance['sessions'] >= 2) & 
            (source_performance['revenue_per_session'] < 500)
        ]
        if not low_efficiency.empty:
            report.append("### ⚠️ 改善が必要な流入元")
            for _, row in low_efficiency.iterrows():
                report.append(f"- **{row['source']}**: セッションあたり {row['revenue_per_session']:,.0f}円")
            report.append("")
    
    # 商品・流入元相関分析
    if product_source is not None and not product_source.empty:
        report.append("## 🛍️ 商品・流入元相関分析")
        report.append("")
        report.append("### 売上トップ5商品")
        for i, row in product_source.iterrows():
            report.append(f"{i+1}. **{row['product_title']}**: {row['total_price']:,.0f}円")
        report.append("")
        
        report.append("### 流入元別の商品傾向")
        report.append("- **直接アクセス**: リピーター、ブランド認知度が高い")
        report.append("- **Instagram**: ビジュアル重視、若年層・女性層")
        report.append("- **Google**: 検索意図が明確、比較検討段階")
        report.append("")
    
    # コンバージョンファネル分析
    if conversion_funnel is not None and not conversion_funnel.empty:
        report.append("## 🎯 コンバージョンファネル分析")
        report.append("")
        
        # ファネル段階
        report.append("### ファネル段階別の流入元")
        
        awareness_sources = conversion_funnel[
            (conversion_funnel['sessions'] >= 3) & 
            (conversion_funnel['revenue_per_session'] < 1000)
        ]
        if not awareness_sources.empty:
            report.append("#### 1️⃣ 認知段階（ブランド認知）")
            for _, row in awareness_sources.iterrows():
                report.append(f"- {row['source']}: {row['sessions']}セッション")
        
        purchase_sources = conversion_funnel[
            conversion_funnel['revenue_per_session'] >= 3000
        ]
        if not purchase_sources.empty:
            report.append("#### 3️⃣ 購入段階（決断・購入）")
            for _, row in purchase_sources.iterrows():
                report.append(f"- {row['source']}: セッションあたり{row['revenue_per_session']:,.0f}円")
        report.append("")
    
    # アクションプラン
    report.append("## 🚀 推奨アクションプラン")
    report.append("")
    
    if source_performance is not None and not source_performance.empty:
        # 高効率流入元の強化
        high_efficiency = source_performance[source_performance['revenue_per_session'] > 1000]
        if not high_efficiency.empty:
            report.append("### 🔥 高効率流入元の強化")
            for _, row in high_efficiency.iterrows():
                report.append(f"- **{row['source']}**: 予算増額、ターゲティング拡大")
            report.append("")
        
        # 低効率流入元の改善
        low_efficiency = source_performance[
            (source_performance['sessions'] >= 2) & 
            (source_performance['revenue_per_session'] < 500)
        ]
        if not low_efficiency.empty:
            report.append("### ⚡ 低効率流入元の改善")
            for _, row in low_efficiency.iterrows():
                report.append(f"- **{row['source']}**: ランディングページ最適化、ターゲティング見直し")
            report.append("")
    
    # レポートを保存
    report_filename = f"cross_analysis_30days_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    print(f"クロス分析レポートを {report_filename} に保存しました。")
    
    # レポート内容を表示
    print("\n" + "="*60)
    print("クロス分析レポート")
    print("="*60)
    for line in report:
        print(line)

def main():
    """メイン処理を実行します。"""
    print("🔍 直近30日 売上・GAデータ クロス分析を開始します...")
    
    # 最新のCSVファイルを検索
    files = find_latest_csv_files()
    
    if not files:
        print("分析対象のCSVファイルが見つかりません。")
        print("先にShopifyデータ取得とGoogle Analyticsデータ取得を実行してください。")
        return
    
    print(f"見つかったファイル: {files}")
    
    # データを読み込み
    data = load_data(files)
    
    if not data:
        print("データの読み込みに失敗しました。")
        return
    
    # 各種分析を実行
    daily_trends = analyze_daily_trends(
        data.get('orders', pd.DataFrame()),
        data.get('ga4', pd.DataFrame())
    )
    
    source_performance = analyze_source_performance(
        data.get('orders', pd.DataFrame()),
        data.get('ga4', pd.DataFrame())
    )
    
    product_source = analyze_product_source_correlation(
        data.get('orders', pd.DataFrame()),
        data.get('products', pd.DataFrame()),
        data.get('ga4', pd.DataFrame())
    )
    
    conversion_funnel = analyze_conversion_funnel(
        data.get('ga4', pd.DataFrame())
    )
    
    # クロス分析レポート生成
    generate_cross_analysis_report(
        daily_trends, source_performance, product_source, conversion_funnel
    )
    
    print("\n🎉 クロス分析が完了しました！")
    print("生成されたレポートファイルを確認し、ビジネスインサイトを活用してください。")

if __name__ == "__main__":
    main()
