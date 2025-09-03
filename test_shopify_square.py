#!/usr/bin/env python3
"""
ShopifyとSquareのデータ取得テスト
"""

import os
from dotenv import load_dotenv
from src.connectors.shopify import fetch_shopify_all_incremental
from src.connectors.square import fetch_square_all

def test_shopify_connection():
    """Shopify接続をテスト"""
    try:
        # 環境変数を読み込み
        load_dotenv()
        
        print("🛍️  Shopify接続テスト開始...")
        
        # Shopifyデータを取得（最新の1日分）
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        print(f"📅 取得期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
        
        # Shopifyデータを取得
        shopify_data = fetch_shopify_all_incremental(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        print("✅ Shopify接続成功")
        print(f"📊 注文数: {len(shopify_data.get('orders', []))}")
        print(f"📦 商品数: {len(shopify_data.get('products', []))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Shopify接続エラー: {e}")
        return False

def test_square_connection():
    """Square接続をテスト"""
    try:
        # 環境変数を読み込み
        load_dotenv()
        
        print("💳 Square接続テスト開始...")
        
        # Squareデータを取得（最新の1日分）
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        print(f"📅 取得期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
        
        # Squareデータを取得
        square_data = fetch_square_all(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        print("✅ Square接続成功")
        print(f"💰 支払い数: {len(square_data.get('payments', []))}")
        print(f"🔄 返金数: {len(square_data.get('refunds', []))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Square接続エラー: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Shopify & Square接続テスト")
    print("=" * 40)
    
    # Shopifyテスト
    print("\n1️⃣ Shopify接続テスト")
    shopify_success = test_shopify_connection()
    
    # Squareテスト
    print("\n2️⃣ Square接続テスト")
    square_success = test_square_connection()
    
    # 結果サマリー
    print("\n📊 テスト結果サマリー")
    print("=" * 30)
    print(f"Shopify: {'✅ 成功' if shopify_success else '❌ 失敗'}")
    print(f"Square:  {'✅ 成功' if square_success else '❌ 失敗'}")
    
    if shopify_success and square_success:
        print("\n🎉 両方の接続が成功しました！")
        print("次のステップ: データベースに保存してダッシュボードを起動")
    else:
        print("\n⚠️  一部の接続に失敗しました")
        print("環境変数の設定を確認してください")
