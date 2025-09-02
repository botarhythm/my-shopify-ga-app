"""
GA4 データ取得コネクタ
実データ取得によるGA4統合
"""
from google.analytics.data_v1beta import BetaAnalyticsDataClient, DateRange, Metric, Dimension, RunReportRequest
import os
import pandas as pd
import datetime as dt
from typing import List, Optional


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
    client = BetaAnalyticsDataClient()
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in metrics],
    )
    
    try:
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
    dims = ["date", "source", "sessionDefaultChannelGroup", "pagePath"]
    metrics = ["sessions", "totalUsers", "purchases", "purchaseRevenue"]
    
    property_id = os.getenv("GA4_PROPERTY_ID")
    if not property_id:
        raise ValueError("GA4_PROPERTY_ID 環境変数が設定されていません")
    
    return fetch_ga4(property_id, start, end, dims, metrics)


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
