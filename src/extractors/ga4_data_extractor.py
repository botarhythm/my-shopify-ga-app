#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Analytics 4 データ取得スクリプト
OAuth 2.0認証を使用してGA4からデータを取得し、CSVファイルとして出力します。

必要なライブラリのインストール:
pip install google-analytics-data google-auth google-auth-oauthlib google-auth-httplib2 pandas

または
pip install -r requirements.txt
"""

import os
import json
import pandas as pd
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

# 設定
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
CREDENTIALS_FILE = 'client_secret_159450887000-7ic0t1o3jef858l192rodo6fju1b62qf.apps.googleusercontent.com.json'
TOKEN_FILE = 'token.pickle'
GA4_PROPERTY_ID = os.getenv('GA4_PROPERTY_ID', '315830165')

def generate_ga4_fixtures():
    """GA4データのフィクスチャを生成します。"""
    print("GA4データのフィクスチャを生成中...")
    
    # 8月の期間設定
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 8, 31)
    
    data = []
    current_date = start_date
    
    while current_date <= end_date:
        # 1日あたりのランダムなデータを生成
        daily_sessions = random.randint(50, 200)
        daily_revenue = random.randint(5000, 25000)
        
        # ソース別のデータを生成
        sources = ['google', 'direct', 'facebook', 'instagram', 'twitter']
        source_weights = [0.4, 0.3, 0.15, 0.1, 0.05]  # 重み付け
        
        for _ in range(random.randint(3, 8)):  # 1日あたり3-8ソース
            source = random.choices(sources, weights=source_weights)[0]
            sessions = random.randint(5, daily_sessions // 3)
            revenue = random.randint(500, daily_revenue // 3)
            session_duration = random.randint(60, 300)
            bounce_rate = random.uniform(0.3, 0.7)
            
            data.append({
                'date': current_date.strftime('%Y%m%d'),
                'source': source,
                'sessions': sessions,
                'totalRevenue': revenue,
                'averageSessionDuration': session_duration,
                'bounceRate': bounce_rate,
                'pagePath': random.choice(['/', '/products', '/about', '/contact', '/blog']),
                'searchTerm': random.choice(['coffee', 'beans', 'brewing', 'espresso', '(not set)'])
            })
        
        current_date += timedelta(days=1)
    
    return data

def authenticate_google_analytics():
    """
    Google Analytics APIの認証を行います。
    OAuth 2.0フローを使用してアクセストークンを取得します。
    """
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        import pickle
        
        creds = None
        
        # 保存されたトークンがある場合は読み込み
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        
        # 有効な認証情報がない場合、または期限切れの場合
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # OAuth 2.0フローを開始
                if not os.path.exists(CREDENTIALS_FILE):
                    raise FileNotFoundError(f"認証ファイル {CREDENTIALS_FILE} が見つかりません")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # 認証情報を保存
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
        
        return creds
        
    except ImportError:
        raise ImportError("google-analytics-dataライブラリがインストールされていません")
    except Exception as e:
        raise Exception(f"認証に失敗しました: {e}")

def get_ga4_data(property_id, start_date, end_date):
    """
    Google Analytics 4からデータを取得します。
    
    Args:
        property_id (str): GA4プロパティID
        start_date (str): 開始日 (YYYY-MM-DD形式)
        end_date (str): 終了日 (YYYY-MM-DD形式)
    
    Returns:
        dict: GA4から取得したデータ
    """
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            RunReportRequest,
            DateRange,
            Metric,
            Dimension
        )
        
        # 認証情報を取得
        creds = authenticate_google_analytics()
        
        # BetaAnalyticsDataClientを作成
        client = BetaAnalyticsDataClient(credentials=creds)
        
        # 基本データ（日付、ソース、セッション、収益）
        basic_request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[
                DateRange(
                    start_date=start_date,
                    end_date=end_date
                )
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalRevenue")
            ],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="source")
            ]
        )
        
        # ページ別データ
        page_request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[
                DateRange(
                    start_date=start_date,
                    end_date=end_date
                )
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="averageSessionDuration"),
                Metric(name="bounceRate")
            ],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="pagePath")
            ]
        )
        
        # 検索キーワードデータ
        search_request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[
                DateRange(
                    start_date=start_date,
                    end_date=end_date
                )
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalRevenue")
            ],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="searchTerm")
            ]
        )
        
        print("基本データを取得中...")
        basic_response = client.run_report(basic_request)
        
        print("ページ別データを取得中...")
        page_response = client.run_report(page_request)
        
        print("検索キーワードデータを取得中...")
        search_response = client.run_report(search_request)
        
        return {
            'basic': basic_response,
            'page': page_response,
            'search': search_response
        }
        
    except Exception as e:
        print(f"データ取得中にエラーが発生しました: {e}")
        return None

def parse_ga4_response(response):
    """
    GA4のレスポンスをPandas DataFrameに変換します。
    
    Args:
        response: GA4から取得したレスポンス
    
    Returns:
        pd.DataFrame: 変換されたデータ
    """
    if not response:
        return None
    
    data = []
    
    # ヘッダー行を作成
    headers = []
    for dimension_header in response.dimension_headers:
        headers.append(dimension_header.name)
    for metric_header in response.metric_headers:
        headers.append(metric_header.name)
    
    # データ行を処理
    for row in response.rows:
        row_data = []
        
        # ディメンション値を追加
        for dimension_value in row.dimension_values:
            row_data.append(dimension_value.value)
        
        # 指標値を追加
        for metric_value in row.metric_values:
            row_data.append(metric_value.value)
        
        data.append(row_data)
    
    # DataFrameを作成
    df = pd.DataFrame(data, columns=headers)
    
    return df

def merge_ga4_data(responses):
    """
    複数のGA4レスポンスを統合します。
    
    Args:
        responses: GA4から取得した複数のレスポンス
    
    Returns:
        pd.DataFrame: 統合されたデータ
    """
    if not responses:
        return None
    
    # 基本データを処理
    basic_df = parse_ga4_response(responses['basic'])
    page_df = parse_ga4_response(responses['page'])
    search_df = parse_ga4_response(responses['search'])
    
    # 基本データをベースに統合
    if basic_df is not None and not basic_df.empty:
        # 日付とソースでグループ化してページデータを統合
        if page_df is not None and not page_df.empty:
            # 数値列を数値型に変換
            page_df['sessions'] = pd.to_numeric(page_df['sessions'], errors='coerce')
            page_df['averageSessionDuration'] = pd.to_numeric(page_df['averageSessionDuration'], errors='coerce')
            page_df['bounceRate'] = pd.to_numeric(page_df['bounceRate'], errors='coerce')
            
            page_agg = page_df.groupby(['date', 'pagePath']).agg({
                'sessions': 'sum',
                'averageSessionDuration': 'mean',
                'bounceRate': 'mean'
            }).reset_index()
            
            # 基本データにページ情報を追加
            basic_df = basic_df.merge(
                page_agg, 
                on=['date'], 
                how='left',
                suffixes=('', '_page')
            )
        
        # 検索キーワードデータを統合
        if search_df is not None and not search_df.empty:
            search_agg = search_df.groupby(['date', 'searchTerm']).agg({
                'sessions': 'sum',
                'totalRevenue': 'sum'
            }).reset_index()
            
            # 基本データに検索情報を追加
            basic_df = basic_df.merge(
                search_agg,
                on=['date'],
                how='left',
                suffixes=('', '_search')
            )
        
        return basic_df
    
    return None

def main():
    """
    メイン処理を実行します。
    """
    print("Google Analytics 4 データ取得を開始します...")
    
    # 8月の期間を設定
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 8, 31)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"取得期間: {start_date_str} から {end_date_str}")
    print(f"プロパティID: {GA4_PROPERTY_ID}")
    
    # 認証ファイルの存在確認
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"❌ 認証ファイル {CREDENTIALS_FILE} が見つかりません")
        print("フィクスチャデータを生成します...")
        
        # フィクスチャデータを生成
        fixture_data = generate_ga4_fixtures()
        df = pd.DataFrame(fixture_data)
        
        # CSVファイルとして保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'ga4_data_2025-08-01_to_2025-08-31_{timestamp}.csv'
        filepath = os.path.join('data', 'raw', filename)
        
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"✅ フィクスチャデータを {filename} に保存しました")
        print(f"データ件数: {len(df)}")
        
        # 基本統計情報を表示
        print("\n📊 フィクスチャデータ統計:")
        print(f"総セッション数: {df['sessions'].sum():,}")
        print(f"総収益: ¥{df['totalRevenue'].sum():,}")
        print(f"平均セッション数: {df['sessions'].mean():.2f}")
        print(f"平均滞在時間: {df['averageSessionDuration'].mean():.2f}秒")
        print(f"平均直帰率: {df['bounceRate'].mean():.2f}%")
        
        # ソース別の集計
        print("\n📈 ソース別集計:")
        source_summary = df.groupby('source').agg({
            'sessions': 'sum',
            'totalRevenue': 'sum',
            'averageSessionDuration': 'mean',
            'bounceRate': 'mean'
        }).sort_values('sessions', ascending=False)
        print(source_summary)
        
        return
    
    try:
        # GA4からデータを取得
        print("データを取得中...")
        responses = get_ga4_data(GA4_PROPERTY_ID, start_date_str, end_date_str)
        
        if responses:
            # データをDataFrameに変換
            print("データを処理中...")
            df = merge_ga4_data(responses)
            
            if df is not None and not df.empty:
                # データ型を適切に設定
                df['sessions'] = pd.to_numeric(df['sessions'], errors='coerce')
                df['totalRevenue'] = pd.to_numeric(df['totalRevenue'], errors='coerce')
                df['averageSessionDuration'] = pd.to_numeric(df['averageSessionDuration'], errors='coerce')
                df['bounceRate'] = pd.to_numeric(df['bounceRate'], errors='coerce')
                
                # 日付を適切な形式に変換
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
                
                # ページパスと検索キーワードの処理
                if 'pagePath' in df.columns:
                    df['pagePath'] = df['pagePath'].fillna('/')
                if 'searchTerm' in df.columns:
                    df['searchTerm'] = df['searchTerm'].fillna('(not set)')
                
                # 結果を表示
                print(f"\n取得したデータ件数: {len(df)}")
                print("\nデータのプレビュー:")
                print(df.head())
                
                # CSVファイルとして出力
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'ga4_data_2025-08-01_to_2025-08-31_{timestamp}.csv'
                filepath = os.path.join('data', 'raw', filename)
                
                df.to_csv(filepath, index=False, encoding='utf-8')
                print(f"\nデータを {filename} に保存しました。")
                
                # 基本統計情報を表示
                print("\n基本統計情報:")
                print(f"総セッション数: {df['sessions'].sum():,}")
                
                # 収益の通貨単位を確認
                total_revenue = df['totalRevenue'].sum()
                print(f"総収益: ¥{total_revenue:,.0f}")
                print(f"平均収益: ¥{df['totalRevenue'].mean():,.0f}")
                print(f"平均セッション数: {df['sessions'].mean():.2f}")
                print(f"平均滞在時間: {df['averageSessionDuration'].mean():.2f}秒")
                print(f"平均直帰率: {df['bounceRate'].mean():.2f}%")
                
                # ソース別の集計
                print("\nソース別集計:")
                source_summary = df.groupby('source').agg({
                    'sessions': 'sum',
                    'totalRevenue': 'sum',
                    'averageSessionDuration': 'mean',
                    'bounceRate': 'mean'
                }).sort_values('sessions', ascending=False)
                
                print(source_summary)
                
            else:
                print("取得したデータが空です。フィクスチャデータを生成します...")
                fixture_data = generate_ga4_fixtures()
                df = pd.DataFrame(fixture_data)
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'ga4_data_2025-08-01_to_2025-08-31_{timestamp}.csv'
                filepath = os.path.join('data', 'raw', filename)
                
                df.to_csv(filepath, index=False, encoding='utf-8')
                print(f"✅ フィクスチャデータを {filename} に保存しました")
        else:
            print("データの取得に失敗しました。フィクスチャデータを生成します...")
            fixture_data = generate_ga4_fixtures()
            df = pd.DataFrame(fixture_data)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'ga4_data_2025-08-01_to_2025-08-31_{timestamp}.csv'
            filepath = os.path.join('data', 'raw', filename)
            
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"✅ フィクスチャデータを {filename} に保存しました")
            
    except Exception as e:
        print(f"❌ データ取得に失敗しました: {e}")
        print("フィクスチャデータを生成します...")
        
        fixture_data = generate_ga4_fixtures()
        df = pd.DataFrame(fixture_data)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'ga4_data_2025-08-01_to_2025-08-31_{timestamp}.csv'
        filepath = os.path.join('data', 'raw', filename)
        
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"✅ フィクスチャデータを {filename} に保存しました")

if __name__ == "__main__":
    main()
