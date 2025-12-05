"""
GA4 データ取得コネクタ
OAuth認証によるGA4統合
"""
from google.analytics.data_v1beta import BetaAnalyticsDataClient, DateRange, Metric, Dimension, RunReportRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pandas as pd
import datetime as dt
from typing import List, Optional
import pickle


# OAuth 2.0 スコープ
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


def get_ga4_credentials():
    """
    GA4用のOAuth認証情報を取得
    """
    creds = None
    token_path = os.getenv("GA4_TOKEN_PATH", "./data/raw/ga4_token.pickle")
    
    # 既存のトークンを読み込み
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    # トークンが無効または存在しない場合
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # OAuth 2.0 フローを開始
            client_secret_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if not client_secret_path or not os.path.exists(client_secret_path):
                raise ValueError("OAuth クライアントシークレットファイルが見つかりません")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # トークンを保存
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def fetch_ga4(property_id: str, start: str, end: str, dims: List[str], metrics: List[str]) -> pd.DataFrame:
    """
    GA4 APIからデータを取得
    
    Args:
        property_id: GA4プロパティID
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
        dims: ディメンションリスト
        metrics: メトリクスリスト
    
    Returns:
        DataFrame: GA4データ
    """
    try:
        # OAuth認証情報を取得
        creds = get_ga4_credentials()
        client = BetaAnalyticsDataClient(credentials=creds)
        
        req = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start, end_date=end)],
            dimensions=[Dimension(name=d) for d in dims],
            metrics=[Metric(name=m) for m in metrics],
        )
        
        resp = client.run_report(req)
        rows = []
        for r in resp.rows:
            row = {}
            # ディメンション値を追加
            for d, v in zip(dims, [c.value for c in r.dimension_values]):
                row[d] = v
            # メトリクス値を追加
            for m, c in zip(metrics, r.metric_values):
                row[m] = float(c.value or 0)
            
            # DuckDBスキーマに合わせて列名を変換
            row["channel"] = row.pop("sessionDefaultChannelGroup", "")
            row["page_path"] = row.pop("pagePath", "")
            row["purchases"] = row.pop("transactions", 0)
            row["revenue"] = row.pop("totalRevenue", 0)
            row["users"] = row.pop("totalUsers", 0)
            
            # 日付フォーマットを修正（YYYYMMDD -> YYYY-MM-DD）
            if "date" in row and row["date"]:
                date_str = str(row["date"])
                if len(date_str) == 8 and date_str.isdigit():
                    row["date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # DuckDBスキーマの順序に合わせて列を並び替え
            ordered_row = {
                "date": row.get("date"),
                "source": row.get("source"),
                "channel": row.get("channel"),
                "page_path": row.get("page_path"),
                "sessions": row.get("sessions", 0),
                "users": row.get("users", 0),
                "purchases": row.get("purchases", 0),
                "revenue": row.get("revenue", 0),
                "transactions": row.get("transactions", 0)
            }
            row = ordered_row
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    except Exception as e:
        print(f"GA4 API エラー: {e}")
        return pd.DataFrame()


def fetch_ga4_daily_all(start: str, end: str) -> pd.DataFrame:
    """
    日別GA4データを全メトリクスで取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: 日別GA4データ
    """
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise ValueError("GA4_PROPERTY_ID 環境変数が設定されていません")
    
    # 基本メトリクス（transactions以外）
    dims_basic = ["date", "source", "sessionDefaultChannelGroup", "pagePath"]
    metrics_basic = ["sessions", "totalUsers", "totalRevenue"]
    
    df_basic = fetch_ga4(property_id, start, end, dims_basic, metrics_basic)
    
    # transactionsメトリクス（別クエリ）
    dims_trans = ["date", "source", "sessionDefaultChannelGroup", "pagePath"]
    metrics_trans = ["transactions"]
    
    df_trans = fetch_ga4(property_id, start, end, dims_trans, metrics_trans)
    
    # データをマージ
    if not df_basic.empty and not df_trans.empty:
        # transactions列を追加
        df_basic["transactions"] = df_trans.get("transactions", 0)
    elif not df_basic.empty:
        df_basic["transactions"] = 0
    else:
        return pd.DataFrame()
    
    return df_basic


def fetch_ga4_yoy(start: str, end: str) -> pd.DataFrame:
    """
    前年同期データを取得
    
    Args:
        start: 当年開始日 (YYYY-MM-DD)
        end: 当年終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: 前年同期データ
    """
    # 前年同期期間を計算
    start_date = dt.datetime.strptime(start, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end, "%Y-%m-%d")
    
    prev_start = (start_date - dt.timedelta(days=365)).strftime("%Y-%m-%d")
    prev_end = (end_date - dt.timedelta(days=365)).strftime("%Y-%m-%d")
    
    df = fetch_ga4_daily_all(prev_start, prev_end)
    if not df.empty:
        # 前年データであることを示す列を追加
        df['is_previous_year'] = True
        # 日付を当年に調整（YoY比較用）
        df['date'] = df['date'].apply(lambda x: 
            (dt.datetime.strptime(x, "%Y-%m-%d") + dt.timedelta(days=365)).strftime("%Y-%m-%d"))
    
    return df


def fetch_ga4_search_terms(start: str, end: str) -> pd.DataFrame:
    """
    検索語データを取得（SEO分析用）
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: 検索語データ
    """
    dims = ["date", "searchTerm"]
    metrics = ["sessions", "totalUsers", "purchases", "purchaseRevenue"]
    
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise ValueError("GA4_PROPERTY_ID 環境変数が設定されていません")
    
    return fetch_ga4(property_id, start, end, dims, metrics)
