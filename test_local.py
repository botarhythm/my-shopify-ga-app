#!/usr/bin/env python3
"""
ローカルテストスクリプト
DuckDBスキーマ初期化とETL処理を自動実行
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def run_command(cmd, description):
    """コマンドを実行して結果を表示"""
    print(f"\n{'='*50}")
    print(f"実行中: {description}")
    print(f"コマンド: {cmd}")
    print(f"{'='*50}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"✅ {description} 成功")
            return True
        else:
            print(f"❌ {description} 失敗 (終了コード: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"❌ {description} エラー: {e}")
        return False

def check_environment():
    """環境変数の確認"""
    # .envファイルを明示的に読み込み
    from dotenv import load_dotenv
    load_dotenv()
    
    print("🔍 環境変数確認")
    print(f"  DUCKDB_PATH: {os.getenv('DUCKDB_PATH', '未設定')}")
    print(f"  GA4_PROPERTY_ID: {os.getenv('GA4_PROPERTY_ID', '未設定')}")
    print(f"  GOOGLE_ADS_CUSTOMER_ID: {os.getenv('GOOGLE_ADS_CUSTOMER_ID', '未設定')}")
    print(f"  SHOPIFY_ACCESS_TOKEN: {'設定済み' if os.getenv('SHOPIFY_ACCESS_TOKEN') else '未設定'}")
    print(f"  SQUARE_ACCESS_TOKEN: {'設定済み' if os.getenv('SQUARE_ACCESS_TOKEN') else '未設定'}")

def main():
    """メイン処理"""
    print("🚀 ローカルテスト開始")
    print(f"📁 作業ディレクトリ: {os.getcwd()}")
    
    # 環境変数確認
    check_environment()
    
    # 1. DuckDBスキーマ初期化
    success1 = run_command(
        "python scripts/bootstrap_duckdb.py",
        "DuckDBスキーマ初期化"
    )
    
    if not success1:
        print("❌ スキーマ初期化に失敗しました")
        return False
    
    # 2. ETL処理実行（実際のデータ取得）
    success2 = run_command(
        "python scripts/run_etl.py",
        "ETL処理（実際のデータ取得）"
    )
    
    if not success2:
        print("❌ ETL処理に失敗しました")
        return False
    
    # 3. ヘルスチェック
    success3 = run_command(
        "python scripts/health_check.py",
        "ヘルスチェック"
    )
    
    if not success3:
        print("❌ ヘルスチェックに失敗しました")
        return False
    
    # 4. Streamlitアプリ起動
    print(f"\n{'='*50}")
    print("🎉 ローカルテスト完了！")
    print("Streamlitアプリを起動します...")
    print(f"{'='*50}")
    
    # Streamlitアプリを起動
    try:
        subprocess.run("streamlit run streamlit_app.py", shell=True)
    except KeyboardInterrupt:
        print("\n👋 Streamlitアプリを停止しました")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("\n✅ ローカルテストが正常に完了しました")
        else:
            print("\n❌ ローカルテストでエラーが発生しました")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ 予期しないエラー: {e}")
        sys.exit(1)
