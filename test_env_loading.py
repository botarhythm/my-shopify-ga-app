#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
環境変数読み込みテストスクリプト
.envファイルから環境変数を読み込んでGoogle Ads APIの動作確認
"""

import os
from dotenv import load_dotenv
import sys

def load_environment_variables():
    """環境変数を読み込む"""
    # .envファイルを読み込み
    load_dotenv()
    
    # 必要な環境変数を確認
    required_vars = [
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET", 
        "GOOGLE_ADS_REFRESH_TOKEN",
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CUSTOMER_ID"
    ]
    
    print("=== 環境変数設定状況 ===")
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # セキュリティのため、最初の10文字のみ表示
            display_value = value[:10] + "..." if len(value) > 10 else value
            print(f"✅ {var}: {display_value}")
        else:
            print(f"❌ {var}: 未設定")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n❌ 不足している環境変数: {missing_vars}")
        return False
    
    print("\n✅ すべての環境変数が設定されています")
    return True

def test_google_ads_client():
    """Google Ads APIクライアントのテスト"""
    try:
        print("\n=== Google Ads APIクライアントテスト ===")
        
        # srcディレクトリをパスに追加
        sys.path.append('src')
        
        from ads.google_ads_client import GoogleAdsClientFactory
        
        # 設定ファイルのパス
        config_path = "config/google_ads.yaml"
        
        # クライアントファクトリを作成
        factory = GoogleAdsClientFactory(config_path)
        
        # MCCベーシック対応の確認
        if factory.is_basic_mcc():
            print("🔧 MCCベーシックアカウントとして認識されました")
            restrictions = factory.get_mcc_restrictions()
            if restrictions.get("rate_limit_conservative"):
                print("📊 レート制限対応: 有効")
            if restrictions.get("advanced_features_disabled"):
                print("🚫 高度な機能制限: 有効")
        else:
            print("🔧 標準MCCアカウントとして認識されました")
        
        # クライアント作成テスト
        print("\n🔄 Google Ads APIクライアントを作成中...")
        client = factory.create_client()
        print("✅ Google Ads APIクライアントの作成に成功しました")
        
        # 顧客IDの確認
        customer_id = factory.get_customer_id()
        print(f"👤 顧客ID: {customer_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Google Ads APIクライアントテストでエラーが発生しました: {e}")
        return False

def test_data_fetcher():
    """データ取得機能のテスト"""
    try:
        print("\n=== データ取得機能テスト ===")
        
        from ads.fetch_ads import GoogleAdsDataFetcher
        
        # データ取得オブジェクトを作成
        fetcher = GoogleAdsDataFetcher()
        print("✅ データ取得オブジェクトの作成に成功しました")
        
        # MCCベーシック対応の確認
        if hasattr(fetcher, 'client_factory') and fetcher.client_factory:
            if fetcher.client_factory.is_basic_mcc():
                print("🔧 MCCベーシック対応のデータ取得オブジェクトとして認識されました")
            else:
                print("🔧 標準MCC対応のデータ取得オブジェクトとして認識されました")
        
        return True
        
    except Exception as e:
        print(f"❌ データ取得機能テストでエラーが発生しました: {e}")
        return False

def main():
    """メイン関数"""
    print("🚀 MCCベーシック対応 Google Ads API テスト開始")
    print("=" * 50)
    
    # 環境変数の読み込み
    if not load_environment_variables():
        print("\n❌ 環境変数の設定が不完全です。.envファイルを確認してください。")
        return
    
    # Google Ads APIクライアントのテスト
    if not test_google_ads_client():
        print("\n❌ Google Ads APIクライアントのテストに失敗しました。")
        return
    
    # データ取得機能のテスト
    if not test_data_fetcher():
        print("\n❌ データ取得機能のテストに失敗しました。")
        return
    
    print("\n" + "=" * 50)
    print("🎉 すべてのテストが完了しました！")
    print("✅ MCCベーシック対応のGoogle Ads APIが正常に動作しています。")

if __name__ == "__main__":
    main()
