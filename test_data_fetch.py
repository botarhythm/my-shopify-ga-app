#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
実際の広告データ取得テストスクリプト
MCCベーシック対応下でのデータ取得動作確認
"""

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys

def test_actual_data_fetch():
    """実際のデータ取得をテスト"""
    try:
        print("🚀 実際の広告データ取得テスト開始")
        print("=" * 50)
        
        # 環境変数を読み込み
        load_dotenv()
        
        # srcディレクトリをパスに追加
        sys.path.append('src')
        
        from ads.fetch_ads import GoogleAdsDataFetcher
        
        # データ取得オブジェクトを作成
        print("🔄 データ取得オブジェクトを作成中...")
        fetcher = GoogleAdsDataFetcher()
        print("✅ データ取得オブジェクトの作成に成功しました")
        
        # MCCベーシック対応の確認
        if hasattr(fetcher, 'client_factory') and fetcher.client_factory:
            if fetcher.client_factory.is_basic_mcc():
                print("🔧 MCCベーシック対応として認識されました")
                restrictions = fetcher.client_factory.get_mcc_restrictions()
                if restrictions.get("rate_limit_conservative"):
                    print("📊 レート制限対応: 有効")
            else:
                print("🔧 標準MCC対応として認識されました")
        
        # テスト用の日付範囲（過去30日）
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        print(f"\n📅 データ取得期間: {start_date} から {end_date}")
        print("🔄 キャンペーンデータの取得を開始...")
        
        # キャンペーンデータの取得テスト
        campaign_data = fetcher.fetch_campaign_data(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        if not campaign_data.empty:
            print(f"✅ キャンペーンデータ取得成功: {len(campaign_data)}行")
            print(f"📊 データ列: {list(campaign_data.columns)}")
            print(f"📅 日付範囲: {campaign_data['date'].min()} から {campaign_data['date'].max()}")
            
            # サンプルデータの表示（最初の3行）
            print("\n📋 サンプルデータ（最初の3行）:")
            print(campaign_data.head(3).to_string(index=False))
        else:
            print("⚠️ キャンペーンデータが空です（期間内にデータがない可能性）")
        
        print("\n🔄 広告グループデータの取得を開始...")
        
        # 広告グループデータの取得テスト
        ad_group_data = fetcher.fetch_ad_group_data(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        if not ad_group_data.empty:
            print(f"✅ 広告グループデータ取得成功: {len(ad_group_data)}行")
            print(f"📊 データ列: {list(ad_group_data.columns)}")
        else:
            print("⚠️ 広告グループデータが空です")
        
        print("\n🔄 キーワードデータの取得を開始...")
        
        # キーワードデータの取得テスト
        keyword_data = fetcher.fetch_keyword_data(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        if not keyword_data.empty:
            print(f"✅ キーワードデータ取得成功: {len(keyword_data)}行")
            print(f"📊 データ列: {list(keyword_data.columns)}")
        else:
            print("⚠️ キーワードデータが空です")
        
        print("\n" + "=" * 50)
        print("🎉 実際のデータ取得テストが完了しました！")
        print("✅ MCCベーシック対応下でGoogle Ads APIが正常に動作しています")
        
        return True
        
    except Exception as e:
        print(f"❌ データ取得テストでエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_actual_data_fetch()
