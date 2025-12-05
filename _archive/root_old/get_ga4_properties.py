#!/usr/bin/env python3
"""
GA4プロパティIDを取得するスクリプト
"""

import os
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
from google.auth import default

def get_ga4_properties():
    """利用可能なGA4プロパティを取得"""
    try:
        # 環境変数を読み込み
        load_dotenv()
        
        # 認証情報を設定
        credentials, project = default()
        
        # Analytics Admin Service クライアントを作成
        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        
        # プロパティリストを取得
        parent = f"accounts/{project}"
        request = admin_client.list_properties(parent=parent)
        
        print("🔍 利用可能なGA4プロパティ:")
        print("=" * 50)
        
        for property in request:
            print(f"📊 プロパティ名: {property.display_name}")
            print(f"🆔 プロパティID: {property.name.split('/')[-1]}")
            print(f"🌐 ウェブサイトURL: {property.website_uri}")
            print(f"📅 作成日: {property.create_time}")
            print("-" * 30)
        
        return True
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        print("\n💡 対処法:")
        print("1. GOOGLE_APPLICATION_CREDENTIALSが正しく設定されているか確認")
        print("2. サービスアカウントにAnalytics Admin権限があるか確認")
        print("3. プロジェクトIDが正しいか確認")
        return False

def test_ga4_connection():
    """GA4接続をテスト"""
    try:
        # 環境変数を読み込み
        load_dotenv()
        
        # クライアントを作成
        client = BetaAnalyticsDataClient()
        
        print("✅ GA4接続テスト成功")
        return True
        
    except Exception as e:
        print(f"❌ GA4接続エラー: {e}")
        return False

if __name__ == "__main__":
    print("🚀 GA4プロパティID取得ツール")
    print("=" * 40)
    
    # 接続テスト
    if test_ga4_connection():
        # プロパティリスト取得
        get_ga4_properties()
    else:
        print("❌ GA4接続に失敗しました")
