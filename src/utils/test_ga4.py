#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Analytics 4 データ取得スクリプトのテスト用スクリプト
基本的な動作確認とライブラリのインポートテストを行います。
"""

def test_imports():
    """必要なライブラリが正しくインポートできるかテストします。"""
    try:
        import pandas as pd
        print("✓ pandas のインポート成功")
        
        from google.oauth2.credentials import Credentials
        print("✓ google.oauth2.credentials のインポート成功")
        
        from google_auth_oauthlib.flow import InstalledAppFlow
        print("✓ google_auth_oauthlib.flow のインポート成功")
        
        from google.auth.transport.requests import Request
        print("✓ google.auth.transport.requests のインポート成功")
        
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        print("✓ google.analytics.data_v1beta のインポート成功")
        
        from google.analytics.data_v1beta.types import (
            RunReportRequest,
            DateRange,
            Metric,
            Dimension
        )
        print("✓ google.analytics.data_v1beta.types のインポート成功")
        
        import pickle
        print("✓ pickle のインポート成功")
        
        return True
        
    except ImportError as e:
        print(f"✗ インポートエラー: {e}")
        return False

def test_file_exists():
    """必要なファイルが存在するかチェックします。"""
    import os
    
    files_to_check = [
        'ga4_data_extractor.py',
        'requirements.txt',
        'client_secret_159450887000-7ic0t1o3jef858l192rodo6fju1b62qf.apps.googleusercontent.com.json'
    ]
    
    all_exist = True
    for file in files_to_check:
        if os.path.exists(file):
            print(f"✓ {file} が存在します")
        else:
            print(f"✗ {file} が存在しません")
            all_exist = False
    
    return all_exist

def test_basic_functionality():
    """基本的な機能をテストします。"""
    try:
        from datetime import datetime, timedelta
        
        # 日付計算のテスト
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        print(f"✓ 日付計算: {start_date_str} から {end_date_str}")
        
        # Pandas DataFrame作成のテスト
        import pandas as pd
        
        test_data = {
            'date': ['2024-01-01', '2024-01-02'],
            'source': ['google', 'direct'],
            'sessions': [100, 150],
            'totalRevenue': [1000.50, 1500.75]
        }
        
        df = pd.DataFrame(test_data)
        print(f"✓ DataFrame作成: {len(df)} 行のデータ")
        
        return True
        
    except Exception as e:
        print(f"✗ 基本機能テストエラー: {e}")
        return False

def main():
    """メインテストを実行します。"""
    print("Google Analytics 4 スクリプトのテストを開始します...\n")
    
    tests = [
        ("ライブラリインポートテスト", test_imports),
        ("ファイル存在チェック", test_file_exists),
        ("基本機能テスト", test_basic_functionality)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"=== {test_name} ===")
        if test_func():
            passed += 1
            print(f"✓ {test_name} 成功\n")
        else:
            print(f"✗ {test_name} 失敗\n")
    
    print(f"=== テスト結果 ===")
    print(f"成功: {passed}/{total}")
    
    if passed == total:
        print("🎉 すべてのテストが成功しました！")
        print("スクリプトを実行する準備が整いました。")
        print("\n次のコマンドで実行してください:")
        print("python ga4_data_extractor.py")
    else:
        print("⚠️  一部のテストが失敗しました。")
        print("エラーメッセージを確認し、必要なライブラリをインストールしてください。")
        print("\nライブラリのインストール:")
        print("pip install -r requirements.txt")

if __name__ == "__main__":
    main()
