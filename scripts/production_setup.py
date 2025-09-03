# scripts/production_setup.py
import os
import sys
import subprocess
from pathlib import Path

def check_env_file():
    """環境変数ファイルの存在確認"""
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .envファイルが見つかりません")
        print("📝 env.templateをコピーして.envファイルを作成してください")
        return False
    
    print("✅ .envファイルが存在します")
    return True

def check_required_files():
    """必要な認証ファイルの存在確認"""
    required_files = [
        "data/raw/ga-sa.json",
        "data/raw/client_secret_*.json"
    ]
    
    missing_files = []
    for pattern in required_files:
        if not list(Path(".").glob(pattern)):
            missing_files.append(pattern)
    
    if missing_files:
        print("❌ 以下の認証ファイルが見つかりません:")
        for file in missing_files:
            print(f"  - {file}")
        print("📝 認証ファイルを配置してください")
        return False
    
    print("✅ 必要な認証ファイルが存在します")
    return True

def run_connection_tests():
    """接続テストを実行"""
    print("\n🔍 接続テストを実行中...")
    
    # GA4接続テスト
    try:
        result = subprocess.run([sys.executable, "test_ga4_connection.py"], 
                               capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ GA4接続テスト: 成功")
        else:
            print(f"❌ GA4接続テスト: 失敗\n{result.stderr}")
    except Exception as e:
        print(f"❌ GA4接続テスト: エラー - {e}")
    
    # Shopify接続テスト
    try:
        result = subprocess.run([sys.executable, "test_shopify_square.py"], 
                               capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ Shopify接続テスト: 成功")
        else:
            print(f"❌ Shopify接続テスト: 失敗\n{result.stderr}")
    except Exception as e:
        print(f"❌ Shopify接続テスト: エラー - {e}")

def run_etl_test():
    """ETLテストを実行"""
    print("\n🔄 ETLテストを実行中...")
    
    try:
        result = subprocess.run([sys.executable, "scripts/run_etl.py"], 
                               capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print("✅ ETLテスト: 成功")
            return True
        else:
            print(f"❌ ETLテスト: 失敗\n{result.stderr}")
            return False
    except Exception as e:
        print(f"❌ ETLテスト: エラー - {e}")
        return False

def run_health_check():
    """ヘルスチェックを実行"""
    print("\n🏥 ヘルスチェックを実行中...")
    
    try:
        result = subprocess.run([sys.executable, "scripts/health_check.py"], 
                               capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ ヘルスチェック: 成功")
            print(result.stdout)
            return True
        else:
            print(f"❌ ヘルスチェック: 失敗\n{result.stderr}")
            return False
    except Exception as e:
        print(f"❌ ヘルスチェック: エラー - {e}")
        return False

def main():
    """本実装セットアップのメイン処理"""
    print("🚀 本実装セットアップ開始")
    print("=" * 50)
    
    # 1. 環境変数ファイル確認
    if not check_env_file():
        print("\n📋 次の手順を実行してください:")
        print("1. cp env.template .env")
        print("2. .envファイルを編集して実際の値を設定")
        print("3. このスクリプトを再実行")
        return
    
    # 2. 認証ファイル確認
    if not check_required_files():
        print("\n📋 次の手順を実行してください:")
        print("1. 必要な認証ファイルをdata/raw/に配置")
        print("2. このスクリプトを再実行")
        return
    
    # 3. 接続テスト
    run_connection_tests()
    
    # 4. ETLテスト
    if run_etl_test():
        # 5. ヘルスチェック
        if run_health_check():
            print("\n🎉 本実装セットアップ完了!")
            print("\n📋 次の手順でStreamlitを起動してください:")
            print("streamlit run streamlit_app.py")
        else:
            print("\n❌ ヘルスチェックに失敗しました")
    else:
        print("\n❌ ETLテストに失敗しました")

if __name__ == "__main__":
    main()
