#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
リフレッシュトークン修正スクリプト
.envファイルのリフレッシュトークンの改行文字を除去
"""

import os
from dotenv import load_dotenv

def fix_refresh_token():
    """リフレッシュトークンの改行文字を除去"""
    
    # .envファイルを読み込み
    load_dotenv()
    
    # 現在のリフレッシュトークンを取得
    current_token = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
    
    if not current_token:
        print("❌ GOOGLE_ADS_REFRESH_TOKENが設定されていません")
        return
    
    print("=== 現在のリフレッシュトークン ===")
    print(f"長さ: {len(current_token)}")
    print(f"内容: {current_token[:50]}...")
    
    # 改行文字を除去
    fixed_token = current_token.replace('\n', '').replace('\r', '').strip()
    
    print("\n=== 修正後のリフレッシュトークン ===")
    print(f"長さ: {len(fixed_token)}")
    print(f"内容: {fixed_token[:50]}...")
    
    # 環境変数に設定
    os.environ["GOOGLE_ADS_REFRESH_TOKEN"] = fixed_token
    
    print("\n✅ リフレッシュトークンを修正しました")
    print("📝 この修正は現在のセッションでのみ有効です")
    print("💡 永続化するには、.envファイルを手動で編集してください")
    
    return fixed_token

def test_google_ads_connection():
    """修正後のトークンでGoogle Ads接続をテスト"""
    try:
        print("\n=== 修正後の接続テスト ===")
        
        # 修正されたトークンで環境変数を設定
        fixed_token = fix_refresh_token()
        if not fixed_token:
            return False
        
        # Google Ads APIクライアントのテスト
        from src.ads.google_ads_client import GoogleAdsClientFactory
        
        config_path = "config/google_ads.yaml"
        factory = GoogleAdsClientFactory(config_path)
        
        # MCCベーシック対応の確認
        if factory.is_basic_mcc():
            print("🔧 MCCベーシックアカウントとして認識されました")
        
        # クライアント作成テスト
        print("🔄 Google Ads APIクライアントを作成中...")
        client = factory.create_client()
        print("✅ Google Ads APIクライアントの作成に成功しました")
        
        return True
        
    except Exception as e:
        print(f"❌ 接続テストでエラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    print("🔧 リフレッシュトークン修正ツール")
    print("=" * 40)
    
    if test_google_ads_connection():
        print("\n🎉 修正が完了し、Google Ads APIに接続できました！")
    else:
        print("\n❌ 修正後も接続できませんでした。")
        print("💡 以下の点を確認してください：")
        print("   1. リフレッシュトークンが正しいか")
        print("   2. OAuth認証が正しく設定されているか")
        print("   3. アカウントの権限設定")
