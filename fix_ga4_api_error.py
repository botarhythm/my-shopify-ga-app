#!/usr/bin/env python3
"""
GA4 API エラー修正版
互換性のあるディメンション・メトリクス組み合わせを使用
"""
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from src.connectors.ga4 import fetch_ga4

# 環境変数を読み込み
load_dotenv()

def fetch_ga4_compatible(start: str, end: str) -> pd.DataFrame:
    """
    GA4 API互換性エラーを回避したデータ取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: GA4データ
    """
    print(f"=== GA4 API 互換性修正版 ===")
    print(f"期間: {start} 〜 {end}")
    
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise ValueError("GA4_PROPERTY_ID 環境変数が設定されていません")
    
    # 互換性のある組み合わせ1: 日別基本メトリクス
    print("\n1. 日別基本メトリクス取得中...")
    try:
        dims1 = ["date"]
        metrics1 = ["sessions", "totalUsers", "totalRevenue"]
        df1 = fetch_ga4(property_id, start, end, dims1, metrics1)
        print(f"  取得成功: {len(df1)}行")
    except Exception as e:
        print(f"  エラー: {e}")
        df1 = pd.DataFrame()
    
    # 互換性のある組み合わせ2: チャンネル別メトリクス
    print("\n2. チャンネル別メトリクス取得中...")
    try:
        dims2 = ["date", "sessionDefaultChannelGroup"]
        metrics2 = ["sessions", "totalUsers", "totalRevenue"]
        df2 = fetch_ga4(property_id, start, end, dims2, metrics2)
        print(f"  取得成功: {len(df2)}行")
    except Exception as e:
        print(f"  エラー: {e}")
        df2 = pd.DataFrame()
    
    # 互換性のある組み合わせ3: ページ別メトリクス（transactions除外）
    print("\n3. ページ別メトリクス取得中...")
    try:
        dims3 = ["date", "pagePath"]
        metrics3 = ["sessions", "totalUsers", "screenPageViews"]
        df3 = fetch_ga4(property_id, start, end, dims3, metrics3)
        print(f"  取得成功: {len(df3)}行")
    except Exception as e:
        print(f"  エラー: {e}")
        df3 = pd.DataFrame()
    
    # 互換性のある組み合わせ4: トランザクション専用
    print("\n4. トランザクション専用取得中...")
    try:
        dims4 = ["date"]
        metrics4 = ["transactions", "totalRevenue"]
        df4 = fetch_ga4(property_id, start, end, dims4, metrics4)
        print(f"  取得成功: {len(df4)}行")
    except Exception as e:
        print(f"  エラー: {e}")
        df4 = pd.DataFrame()
    
    # 結果を統合
    print("\n=== 統合結果 ===")
    if not df1.empty:
        total_sessions = df1['sessions'].sum()
        total_users = df1['users'].sum()
        total_revenue = df1['revenue'].sum()
        print(f"総セッション数: {total_sessions:,}")
        print(f"総ユーザー数: {total_users:,}")
        print(f"総売上: {total_revenue:,.0f}円")
    
    if not df4.empty:
        total_transactions = df4['purchases'].sum()
        print(f"総トランザクション数: {total_transactions:,}")
    
    return df1

def main():
    """メイン実行"""
    try:
        # 2025年8月のデータを取得
        start_date = "2025-08-01"
        end_date = "2025-08-31"
        
        result = fetch_ga4_compatible(start_date, end_date)
        
        if not result.empty:
            print("\nGA4 API エラー解決完了")
        else:
            print("\nデータ取得に失敗しました")
            
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()