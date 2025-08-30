#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopifyストア売上向上分析パイプライン
3つのステップを順次実行し、包括的な分析と戦略提案を行います。

実行手順:
1. Shopify APIからデータ取得
2. データ統合・分析
3. 戦略提案
"""

import os
import sys
import subprocess
import time
from datetime import datetime

def print_header(title):
    """ヘッダーを表示します。"""
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)

def print_step(step_num, step_title):
    """ステップの開始を表示します。"""
    print(f"\n📋 ステップ {step_num}: {step_title}")
    print("-" * 40)

def check_environment():
    """実行環境をチェックします。"""
    print_header("🔍 実行環境チェック")
    
    # 必要なファイルの存在確認
    required_files = [
        'src/extractors/ga4_data_extractor.py',
        'src/extractors/shopify_data_extractor.py', 
        'src/analysis/data_analyzer.py',
        'src/analysis/strategy_proposer.py'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ 不足しているファイル: {missing_files}")
        return False
    
    print("✅ 必要なファイルが揃っています")
    
    # 環境変数の確認
    shopify_token = os.getenv('SHOPIFY_API_TOKEN')
    if not shopify_token:
        print("⚠️  SHOPIFY_API_TOKEN環境変数が設定されていません")
        print("   以下のコマンドで設定してください:")
        print("   set SHOPIFY_API_TOKEN=your_api_token_here")
        return False
    
    print("✅ Shopify APIトークンが設定されています")
    return True

def run_script(script_name, description):
    """Pythonスクリプトを実行します。"""
    print(f"\n🚀 {description}を開始します...")
    print(f"   実行スクリプト: {script_name}")
    
    try:
        # スクリプトを実行
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        if result.returncode == 0:
            print(f"✅ {description}が完了しました")
            if result.stdout:
                print("\n実行結果:")
                print(result.stdout)
        else:
            print(f"❌ {description}でエラーが発生しました")
            if result.stderr:
                print("\nエラー内容:")
                print(result.stderr)
            return False
            
    except Exception as e:
        print(f"❌ スクリプト実行中にエラーが発生: {e}")
        return False
    
    return True

def wait_for_user_input(message):
    """ユーザーの入力を待ちます。"""
    print(f"\n⏸️  {message}")
    input("Enterキーを押して続行してください...")

def main():
    """メインパイプラインを実行します。"""
    print_header("🎯 Shopifyストア売上向上分析パイプライン")
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    
    # 環境チェック
    if not check_environment():
        print("\n❌ 環境チェックに失敗しました。")
        print("必要な設定を行ってから再実行してください。")
        return
    
    print("\n✅ 環境チェックが完了しました")
    
    # ステップ1: Shopifyデータ取得
    print_step(1, "Shopify APIからデータ取得")
    print("注文データと商品データを取得します")
    
    if not run_script('src/extractors/shopify_data_extractor.py', 'Shopifyデータ取得'):
        print("\n❌ Shopifyデータ取得に失敗しました。")
        print("APIトークンやネットワーク接続を確認してください。")
        return
    
    wait_for_user_input("Shopifyデータ取得が完了しました。次のステップに進みますか？")
    
    # ステップ2: データ統合・分析
    print_step(2, "データ統合・分析")
    print("ShopifyデータとGoogle Analyticsデータを統合・分析します")
    
    if not run_script('src/analysis/data_analyzer.py', 'データ統合・分析'):
        print("\n❌ データ統合・分析に失敗しました。")
        print("必要なCSVファイルが存在するか確認してください。")
        return
    
    wait_for_user_input("データ統合・分析が完了しました。次のステップに進みますか？")
    
    # ステップ3: 戦略提案
    print_step(3, "戦略提案")
    print("分析結果を基に売上向上施策を提案します")
    
    if not run_script('src/analysis/strategy_proposer.py', '戦略提案'):
        print("\n❌ 戦略提案に失敗しました。")
        print("分析レポートファイルが存在するか確認してください。")
        return
    
    # 完了メッセージ
    print_header("🎉 パイプライン完了")
    print("すべてのステップが正常に完了しました！")
    print("\n📁 生成されたファイル:")
    
    # 生成されたファイルを一覧表示
    generated_files = []
    for file in os.listdir('data/raw'):
        if any(file.startswith(prefix) for prefix in [
            'shopify_orders_', 'shopify_products_'
        ]):
            generated_files.append(f"data/raw/{file}")
    
    for file in os.listdir('data/reports'):
        if any(file.startswith(prefix) for prefix in [
            'analysis_report_', 'analysis_charts_', 'strategy_report_'
        ]):
            generated_files.append(f"data/reports/{file}")
    
    if generated_files:
        for file in sorted(generated_files):
            print(f"   📄 {file}")
    else:
        print("   生成されたファイルが見つかりません")
    
    print("\n📊 次のステップ:")
    print("1. 生成されたレポートファイルを確認")
    print("2. 優先度の高い施策から実行")
    print("3. 定期的にデータを更新して効果を測定")
    
    print(f"\n完了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  パイプラインが中断されました")
        print("必要に応じて個別のスクリプトを実行してください")
    except Exception as e:
        print(f"\n\n❌ 予期しないエラーが発生しました: {e}")
        print("エラーの詳細を確認し、必要に応じて個別のスクリプトを実行してください")


