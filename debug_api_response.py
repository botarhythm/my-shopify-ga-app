#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads APIレスポンス構造のデバッグスクリプト
実際のデータ構造を確認して、適切なマッピングを特定
"""

import os
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_api_response():
    """APIレスポンスの構造をデバッグ"""
    try:
        print("🔍 Google Ads APIレスポンス構造のデバッグ開始")
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
        
        # テスト用の日付範囲（過去7日）
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        print(f"\n📅 データ取得期間: {start_date} から {end_date}")
        
        # 基本的なクエリを直接実行
        print("\n🔄 基本的なクエリを直接実行中...")
        
        try:
            # クライアントを取得
            client = fetcher._get_client()
            ga_service = client.get_service("GoogleAdsService")
            
            # シンプルなクエリを実行
            simple_query = """
                SELECT 
                    campaign.id,
                    campaign.name,
                    segments.date,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros
                FROM campaign 
                WHERE segments.date BETWEEN '2025-08-01' AND '2025-09-01'
                LIMIT 5
            """
            
            customer_id = fetcher.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID")
            print(f"👤 顧客ID: {customer_id}")
            
            print("🔍 クエリ実行中...")
            response = ga_service.search(customer_id=customer_id, query=simple_query)
            
            print(f"✅ レスポンス取得成功: {len(list(response))}行")
            
            # 最初の行の詳細構造を分析
            print("\n🔍 最初の行の詳細構造分析:")
            for i, row in enumerate(response):
                if i >= 3:  # 最初の3行のみ分析
                    break
                    
                print(f"\n--- 行 {i+1} ---")
                print(f"行の型: {type(row)}")
                print(f"行の属性: {dir(row)}")
                
                # プロトコルバッファの詳細情報
                try:
                    print(f"フィールド情報: {row._fields if hasattr(row, '_fields') else 'N/A'}")
                except:
                    print("フィールド情報: 取得不可")
                
                # 各フィールドの値を確認
                row_dict = {}
                try:
                    # 新しいバージョン用
                    for field in row._fields:
                        value = getattr(row, field)
                        print(f"  {field}: {value} (型: {type(value)})")
                        if hasattr(value, 'value'):
                            print(f"    -> 実際の値: {value.value}")
                            row_dict[field] = value.value
                        else:
                            row_dict[field] = value
                except AttributeError:
                    print("  _fields属性が見つかりません")
                    # 代替方法で属性を確認
                    for attr_name in dir(row):
                        if not attr_name.startswith('_'):
                            try:
                                value = getattr(row, attr_name)
                                print(f"  {attr_name}: {value} (型: {type(value)})")
                                if hasattr(value, 'value'):
                                    print(f"    -> 実際の値: {value.value}")
                                    row_dict[attr_name] = value.value
                                else:
                                    row_dict[attr_name] = value
                            except:
                                continue
                
                print(f"変換後の辞書: {row_dict}")
                
        except Exception as e:
            print(f"❌ クエリ実行でエラー: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "=" * 60)
        print("🎉 デバッグ完了！")
        
        return True
        
    except Exception as e:
        print(f"❌ デバッグでエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_api_response()
