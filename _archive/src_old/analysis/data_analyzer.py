#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShopifyデータとGoogle Analyticsデータの統合・分析スクリプト
売上分析、コンバージョン率分析、流入元分析を行います。

必要なライブラリのインストール:
pip install pandas matplotlib seaborn
"""

import pandas as pd
import os
import glob
from datetime import datetime
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

def analyze_top_products(orders_df, products_df):
    """売上トップ5の商品を分析します。"""
    print("\n=== 売上トップ5商品分析 ===")
    
    if orders_df.empty or products_df.empty:
        print("注文データまたは商品データが不足しています。")
        return None
    
    try:
        # 注文データから商品別売上を集計
        if 'product_title' in orders_df.columns and 'total_price' in orders_df.columns:
            # 商品タイトルと価格で集計
            product_sales = orders_df.groupby('product_title').agg({
                'total_price': 'sum',
                'quantity': 'sum',
                'id': 'count'  # 注文数
            }).reset_index()
            
            product_sales.columns = ['商品名', '総売上', '総数量', '注文数']
            
            # 売上順でソート
            product_sales = product_sales.sort_values('総売上', ascending=False)
            
            # 通貨単位を確認（Shopifyデータは円ベース）
            currency = "円"
            product_sales['総売上'] = product_sales['総売上'].round(0)
            
            print(f"通貨単位: {currency}")
            print("\n売上トップ5商品:")
            print(product_sales.head().to_string(index=False))
            
            return product_sales, currency
            
        else:
            print("注文データに必要な列が含まれていません。")
            return None, None
            
    except Exception as e:
        print(f"商品分析中にエラーが発生: {e}")
        return None, None

def analyze_conversion_pages(ga4_df):
    """コンバージョン率が高いページを分析します。"""
    print("\n=== コンバージョン率分析 ===")
    
    if ga4_df.empty:
        print("Google Analyticsデータが不足しています。")
        return None
    
    try:
        # 現在のGA4データにはページ情報がないため、
        # セッション数と収益の関係から分析
        if 'sessions' in ga4_df.columns and 'totalRevenue' in ga4_df.columns:
            # ソース別のコンバージョン率を計算
            source_conversion = ga4_df.groupby('source').agg({
                'sessions': 'sum',
                'totalRevenue': 'sum'
            }).reset_index()
            
            # コンバージョン率（収益があるセッションの割合）を計算
            source_conversion['conversion_rate'] = (
                source_conversion['totalRevenue'] > 0
            ).astype(int)
            
            # セッション数でソート
            source_conversion = source_conversion.sort_values('sessions', ascending=False)
            
            print("ソース別コンバージョン分析:")
            print(source_conversion.to_string(index=False))
            
            return source_conversion
        else:
            print("Google Analyticsデータに必要な列が含まれていません。")
            return None
            
    except Exception as e:
        print(f"コンバージョン分析中にエラーが発生: {e}")
        return None

def analyze_traffic_sources(ga4_df):
    """流入元と売上の関係を分析します。"""
    print("\n=== 流入元と売上分析 ===")
    
    if ga4_df.empty:
        print("Google Analyticsデータが不足しています。")
        return None
    
    try:
        if 'source' in ga4_df.columns and 'totalRevenue' in ga4_df.columns:
            # 流入元別の売上分析
            traffic_analysis = ga4_df.groupby('source').agg({
                'sessions': 'sum',
                'totalRevenue': 'sum'
            }).reset_index()
            
            # セッションあたりの売上（CVR）を計算
            traffic_analysis['revenue_per_session'] = (
                traffic_analysis['totalRevenue'] / traffic_analysis['sessions']
            ).round(2)
            
            # 売上順でソート
            traffic_analysis = traffic_analysis.sort_values('totalRevenue', ascending=False)
            
            print("流入元別売上分析:")
            print(traffic_analysis.to_string(index=False))
            
            return traffic_analysis
        else:
            print("Google Analyticsデータに必要な列が含まれていません。")
            return None
            
    except Exception as e:
        print(f"流入元分析中にエラーが発生: {e}")
        return None

def create_visualizations(product_sales, source_conversion, traffic_analysis, currency):
    """分析結果の可視化を作成します。"""
    print("\n=== 可視化の作成 ===")
    
    try:
        # 売上トップ5商品のグラフ
        if product_sales is not None and not product_sales.empty:
            plt.figure(figsize=(12, 6))
            top_5 = product_sales.head()
            
            plt.subplot(1, 2, 1)
            plt.barh(top_5['商品名'], top_5['総売上'])
            plt.title(f'売上トップ5商品 ({currency})')
            plt.xlabel('総売上')
            plt.ylabel('商品名')
            
            # 流入元別売上のグラフ
            if traffic_analysis is not None and not traffic_analysis.empty:
                plt.subplot(1, 2, 2)
                top_sources = traffic_analysis.head(8)  # 上位8つの流入元
                plt.barh(top_sources['source'], top_sources['totalRevenue'])
                plt.title(f'流入元別売上 ({currency})')
                plt.xlabel('総売上')
                plt.ylabel('流入元')
            
            plt.tight_layout()
            
            # グラフを保存
            chart_filename = f"analysis_charts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
            print(f"グラフを {chart_filename} に保存しました。")
            
            plt.close()  # グラフを閉じてメモリを解放
            
    except Exception as e:
        print(f"可視化作成中にエラーが発生: {e}")

def generate_analysis_report(product_sales, source_conversion, traffic_analysis, currency):
    """分析レポートを生成します。"""
    print("\n=== 分析レポート生成 ===")
    
    report = []
    report.append("# Shopifyストア分析レポート")
    report.append(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    report.append("")
    
    # 売上トップ5商品
    if product_sales is not None and not product_sales.empty:
        report.append("## 1. 売上トップ5商品")
        report.append("")
        for i, row in product_sales.head().iterrows():
            report.append(f"{i+1}. **{row['商品名']}**")
            report.append(f"   - 総売上: {row['総売上']:,.0f} {currency}")
            report.append(f"   - 総数量: {row['総数量']}個")
            report.append(f"   - 注文数: {row['注文数']}件")
            report.append("")
    
    # 流入元分析
    if traffic_analysis is not None and not traffic_analysis.empty:
        report.append("## 2. 流入元別売上分析")
        report.append("")
        report.append("### 売上上位の流入元:")
        for i, row in traffic_analysis.head(5).iterrows():
            report.append(f"- **{row['source']}**: {row['totalRevenue']:,.0f} {currency} ({row['sessions']}セッション)")
        report.append("")
        
        # セッションあたりの売上が高い流入元
        high_cvr = traffic_analysis[traffic_analysis['sessions'] >= 2].sort_values('revenue_per_session', ascending=False)
        if not high_cvr.empty:
            report.append("### セッションあたり売上が高い流入元:")
            for i, row in high_cvr.head(3).iterrows():
                report.append(f"- **{row['source']}**: セッションあたり {row['revenue_per_session']:,.0f} {currency}")
            report.append("")
    
    # コンバージョン分析
    if source_conversion is not None and not source_conversion.empty:
        report.append("## 3. コンバージョン分析")
        report.append("")
        report.append("### セッション数上位の流入元:")
        for i, row in source_conversion.head(5).iterrows():
            report.append(f"- **{row['source']}**: {row['sessions']}セッション")
        report.append("")
    
    # レポートを保存
    report_filename = f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    print(f"分析レポートを {report_filename} に保存しました。")
    
    # レポート内容を表示
    print("\n" + "="*50)
    print("分析レポート")
    print("="*50)
    for line in report:
        print(line)

def main():
    """メイン処理を実行します。"""
    print("Shopify + Google Analytics データ統合・分析を開始します...")
    
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
    
    # 分析実行
    product_sales, currency = analyze_top_products(
        data.get('orders', pd.DataFrame()),
        data.get('products', pd.DataFrame())
    )
    
    source_conversion = analyze_conversion_pages(
        data.get('ga4', pd.DataFrame())
    )
    
    traffic_analysis = analyze_traffic_sources(
        data.get('ga4', pd.DataFrame())
    )
    
    # 可視化作成
    create_visualizations(product_sales, source_conversion, traffic_analysis, currency)
    
    # 分析レポート生成
    generate_analysis_report(product_sales, source_conversion, traffic_analysis, currency)
    
    print("\nデータ統合・分析が完了しました。")

if __name__ == "__main__":
    main()

