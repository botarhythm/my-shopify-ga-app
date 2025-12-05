#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopifyストア売上向上分析パイプライン
複数のデータソースを統合して包括的な分析を実行します。
"""

import os
import sys
import subprocess
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

def check_environment():
    """実行環境をチェックします。"""
    print("=" * 60)
    print(" 🔍 実行環境チェック")
    print("=" * 60)
    
    # 必要なディレクトリの存在確認
    required_dirs = ['data/raw', 'data/processed', 'data/reports']
    missing_dirs = []
    
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            missing_dirs.append(dir_path)
            os.makedirs(dir_path, exist_ok=True)
    
    if missing_dirs:
        print(f"✅ 不足していたディレクトリを作成しました: {missing_dirs}")
    else:
        print("✅ 必要なファイルが揃っています")
    
    # 環境変数のチェック
    missing_env_vars = []
    
    if not os.getenv('SHOPIFY_API_TOKEN'):
        missing_env_vars.append('SHOPIFY_API_TOKEN')
    
    if not os.getenv('GA4_PROPERTY_ID'):
        missing_env_vars.append('GA4_PROPERTY_ID')
    
    if not os.getenv('SQUARE_ACCESS_TOKEN'):
        missing_env_vars.append('SQUARE_ACCESS_TOKEN')
    
    if not os.getenv('SQUARE_LOCATION_ID'):
        missing_env_vars.append('SQUARE_LOCATION_ID')
    
    if missing_env_vars:
        print(f"⚠️  以下の環境変数が設定されていません: {', '.join(missing_env_vars)}")
        print("   既存のデータファイルを使用して分析を実行します")
        return True  # 既存データで分析を続行
    else:
        print("✅ すべての環境変数が設定されています")
        return True

def check_existing_data():
    """既存のデータファイルをチェックします。"""
    print("\n" + "=" * 60)
    print(" 📊 既存データチェック")
    print("=" * 60)
    
    raw_dir = "data/raw"
    available_data = {}
    
    # Shopifyデータ
    shopify_files = [f for f in os.listdir(raw_dir) if f.startswith("shopify_orders_202508")]
    if shopify_files:
        latest_shopify = max(shopify_files)
        available_data['shopify'] = latest_shopify
        print(f"✅ Shopifyデータ: {latest_shopify}")
    else:
        print("❌ Shopifyデータが見つかりません")
    
    # Squareデータ
    square_files = [f for f in os.listdir(raw_dir) if f.startswith("square_payments_202508")]
    if square_files:
        latest_square = max(square_files)
        available_data['square'] = latest_square
        print(f"✅ Squareデータ: {latest_square}")
    else:
        print("❌ Squareデータが見つかりません")
    
    # GA4データ
    ga4_files = [f for f in os.listdir(raw_dir) if f.startswith("ga4_data_2025-08-01_to_2025-08-31")]
    if ga4_files:
        latest_ga4 = max(ga4_files)
        available_data['ga4'] = latest_ga4
        print(f"✅ GA4データ: {latest_ga4}")
    else:
        print("❌ GA4データが見つかりません")
    
    # Google Adsデータ
    ads_files = [f for f in os.listdir(raw_dir) if f.startswith("google_ads_")]
    if ads_files:
        latest_ads = max(ads_files)
        available_data['ads'] = latest_ads
        print(f"✅ Google Adsデータ: {latest_ads}")
    else:
        print("❌ Google Adsデータが見つかりません")
    
    return available_data

def run_cross_analysis():
    """クロス分析を実行します。"""
    print("\n" + "=" * 60)
    print(" 🔍 クロス分析実行")
    print("=" * 60)
    
    try:
        # 直接スクリプトを実行
        result = subprocess.run([
            sys.executable, 
            'src/analysis/cross_analysis_30days.py'
        ], capture_output=True, text=True, encoding='utf-8')
        
        if result.returncode == 0:
            print("✅ クロス分析が完了しました")
            if result.stdout:
                print("\n実行結果:")
                print(result.stdout)
        else:
            print(f"❌ クロス分析でエラーが発生しました")
            if result.stderr:
                print(f"エラー内容: {result.stderr}")
    except Exception as e:
        print(f"❌ クロス分析でエラーが発生しました: {e}")

def run_complete_analysis():
    """完全統合分析を実行します。"""
    print("\n" + "=" * 60)
    print(" 📊 完全統合分析実行")
    print("=" * 60)
    
    try:
        # 直接スクリプトを実行
        result = subprocess.run([
            sys.executable, 
            'src/utils/analyze_august_complete_data.py'
        ], capture_output=True, text=True, encoding='utf-8')
        
        if result.returncode == 0:
            print("✅ 完全統合分析が完了しました")
            if result.stdout:
                print("\n実行結果:")
                print(result.stdout)
            return {'status': 'success'}
        else:
            print(f"❌ 完全統合分析でエラーが発生しました")
            if result.stderr:
                print(f"エラー内容: {result.stderr}")
            return None
    except Exception as e:
        print(f"❌ 完全統合分析でエラーが発生しました: {e}")
        return None

def generate_summary_report(available_data, analysis_result):
    """サマリーレポートを生成します。"""
    print("\n" + "=" * 60)
    print(" 📋 サマリーレポート生成")
    print("=" * 60)
    
    report_content = f"""
# 🎯 Shopifyストア売上向上分析パイプライン サマリーレポート
生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}

## 📊 利用可能データ
"""
    
    for data_type, filename in available_data.items():
        report_content += f"- **{data_type.upper()}**: {filename}\n"
    
    if analysis_result:
        report_content += f"""
## 📈 分析結果サマリー
- **分析ステータス**: {analysis_result.get('status', 'N/A')}

## 📁 生成されたレポート
- クロス分析レポート: data/reports/cross_analysis_30days_*.md
- 完全統合分析レポート: data/reports/august_complete_analysis_*.md
"""
    
    report_content += f"""
## 🎯 次のステップ
1. **環境変数設定**: 実際のAPIデータ取得のための認証情報設定
2. **リアルタイム分析**: より詳細なクロス分析の実行
3. **ダッシュボード更新**: Streamlitアプリでの最新データ表示
4. **戦略提案**: データに基づくビジネス戦略の提案

## 📊 データ概要
- **Shopify**: オンライン売上データ
- **Square**: 実店舗決済データ
- **GA4**: Webサイトアクセス分析データ
- **Google Ads**: 広告キャンペーンデータ

---
*このレポートは自動生成されました*
"""
    
    # レポート保存
    report_filename = f"data/reports/pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    os.makedirs("data/reports", exist_ok=True)
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"✅ サマリーレポートを {report_filename} に保存しました")

def main():
    """メイン処理を実行します。"""
    print("=" * 60)
    print(" 🎯 Shopifyストア売上向上分析パイプライン")
    print("=" * 60)
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    
    # 環境チェック
    if not check_environment():
        print("\n❌ 環境チェックに失敗しました。")
        print("必要な設定を行ってから再実行してください。")
        return
    
    # 既存データチェック
    available_data = check_existing_data()
    
    if not available_data:
        print("\n❌ 分析可能なデータが見つかりません。")
        print("データ取得を先に実行してください。")
        return
    
    # クロス分析実行
    run_cross_analysis()
    
    # 完全統合分析実行
    analysis_result = run_complete_analysis()
    
    # サマリーレポート生成
    generate_summary_report(available_data, analysis_result)
    
    print("\n" + "=" * 60)
    print(" ✅ 分析パイプラインが完了しました")
    print("=" * 60)
    print("📊 生成されたレポートを確認してください:")
    print("   - data/reports/ ディレクトリ内のファイル")
    print("\n🎯 次のアクション:")
    print("   1. 環境変数を設定して実際のAPIデータを取得")
    print("   2. Streamlitダッシュボードで結果を確認")
    print("   3. ビジネス戦略の検討")

if __name__ == "__main__":
    main()


