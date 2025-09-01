#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI分析による効率的なGoogle広告運用システムのテストスクリプト
実際のキャンペーンデータを取得し、分析基盤を構築
"""

import os
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import logging

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_enhanced_data_fetch():
    """AI分析用の拡張データ取得テスト"""
    try:
        print("🚀 AI分析による効率的なGoogle広告運用システム テスト開始")
        print("=" * 60)
        
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
        
        # より広い期間でのテスト（過去90日）
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90)
        
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
            
            # サンプルデータの表示（最初の5行）
            print("\n📋 サンプルデータ（最初の5行）:")
            print(campaign_data.head(5).to_string(index=False))
            
            # 基本的な統計情報
            print("\n📈 基本的な統計情報:")
            if 'cost_micros' in campaign_data.columns:
                total_cost = campaign_data['cost_micros'].sum() / 1000000  # マイクロ単位から円に変換
                print(f"💰 総費用: ¥{total_cost:,.0f}")
            if 'clicks' in campaign_data.columns:
                total_clicks = campaign_data['clicks'].sum()
                print(f"🖱️ 総クリック数: {total_clicks:,}")
            if 'impressions' in campaign_data.columns:
                total_impressions = campaign_data['impressions'].sum()
                print(f"👁️ 総インプレッション数: {total_impressions:,}")
            
        else:
            print("⚠️ キャンペーンデータが空です")
            print("🔍 より詳細な調査を開始します...")
            
            # 基本的な接続テスト
            test_basic_connection(fetcher)
        
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
        
        print("\n" + "=" * 60)
        print("🎉 AI分析システムの基盤テストが完了しました！")
        
        # 次のステップの提案
        if not campaign_data.empty:
            print("✅ データ取得成功！AI分析システムの構築を開始できます")
            propose_ai_analysis_system(campaign_data)
        else:
            print("⚠️ データ取得に問題があります。設定の見直しが必要です")
        
        return True
        
    except Exception as e:
        print(f"❌ テストでエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_basic_connection(fetcher):
    """基本的なAPI接続テスト"""
    try:
        print("🔍 基本的なAPI接続テストを実行中...")
        
        # アカウント情報の取得テスト
        customer_id = fetcher.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID")
        print(f"👤 顧客ID: {customer_id}")
        
        # クライアントの状態確認
        if fetcher.client:
            print("✅ Google Ads APIクライアント: 接続済み")
        else:
            print("❌ Google Ads APIクライアント: 未接続")
            
    except Exception as e:
        print(f"❌ 基本接続テストでエラー: {e}")

def propose_ai_analysis_system(campaign_data):
    """AI分析システムの提案"""
    print("\n🤖 AI分析による効率的なGoogle広告運用システムの提案")
    print("=" * 50)
    
    print("📊 **実装予定のAI分析機能:**")
    print("1. 🎯 キャンペーンパフォーマンス分析")
    print("   - 自動的なKPI監視")
    print("   - 異常値検出とアラート")
    print("   - 競合分析とベンチマーク")
    
    print("\n2. 💡 最適化提案システム")
    print("   - 予算配分の最適化")
    print("   - 入札額の自動調整")
    print("   - ターゲティング改善案")
    
    print("\n3. 📈 予測分析")
    print("   - 売上予測モデル")
    print("   - 季節性分析")
    print("   - ROI予測")
    
    print("\n4. 🔄 自動化機能")
    print("   - 定期レポート生成")
    print("   - パフォーマンス監視")
    print("   - 最適化提案の自動実行")
    
    print("\n5. 📱 ユーザーフレンドリーなダッシュボード")
    print("   - 直感的な可視化")
    print("   - モバイル対応")
    print("   - リアルタイム更新")

if __name__ == "__main__":
    test_enhanced_data_fetch()
