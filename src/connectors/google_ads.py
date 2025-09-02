"""
Google Ads データ取得コネクタ
GAQLを使用した実データ取得
"""
from google.ads.googleads.client import GoogleAdsClient
import os
import pandas as pd
from typing import List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential


# GAQLクエリテンプレート
GAQL_CAMPAIGN = """
SELECT 
    campaign.id, 
    campaign.name, 
    segments.date,
    metrics.cost_micros, 
    metrics.clicks, 
    metrics.impressions,
    metrics.conversions, 
    metrics.conversions_value
FROM campaign
WHERE segments.date BETWEEN '{start}' AND '{end}'
"""

GAQL_ADGROUP = """
SELECT 
    campaign.id,
    campaign.name,
    ad_group.id,
    ad_group.name,
    segments.date,
    metrics.cost_micros,
    metrics.clicks,
    metrics.impressions,
    metrics.conversions,
    metrics.conversions_value
FROM ad_group
WHERE segments.date BETWEEN '{start}' AND '{end}'
"""

GAQL_KEYWORD = """
SELECT 
    campaign.id,
    campaign.name,
    ad_group.id,
    ad_group.name,
    ad_group_criterion.keyword.text,
    segments.date,
    metrics.cost_micros,
    metrics.clicks,
    metrics.impressions,
    metrics.conversions,
    metrics.conversions_value
FROM keyword_view
WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


def _client():
    """Google Ads クライアントを取得"""
    try:
        return GoogleAdsClient.load_from_env(version="v14")
    except:
        try:
            return GoogleAdsClient.load_from_env(version="v13")
        except:
            return GoogleAdsClient.load_from_env(version="v12")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_gaql(customer_id: str, gaql: str) -> pd.DataFrame:
    """
    GAQLクエリを実行してデータを取得
    
    Args:
        customer_id: Google Ads カスタマーID
        gaql: GAQLクエリ文字列
    
    Returns:
        DataFrame: Google Adsデータ
    """
    client = _client()
    svc = client.get_service("GoogleAdsService")
    
    try:
        stream = svc.search_stream(customer_id=customer_id, query=gaql)
        rows = []
        
        for batch in stream:
            for r in batch.results:
                row = {
                    "date": r.segments.date,
                    "campaign_id": r.campaign.id,
                    "campaign_name": r.campaign.name,
                    "cost": r.metrics.cost_micros / 1_000_000,  # マイクロからドルに変換
                    "clicks": r.metrics.clicks,
                    "impressions": r.metrics.impressions,
                    "conversions": r.metrics.conversions,
                    "conversions_value": r.metrics.conversions_value,
                }
                
                # AdGroup情報があれば追加
                if hasattr(r, 'ad_group') and r.ad_group:
                    row.update({
                        "ad_group_id": r.ad_group.id,
                        "ad_group_name": r.ad_group.name,
                    })
                
                # キーワード情報があれば追加
                if hasattr(r, 'ad_group_criterion') and r.ad_group_criterion:
                    row.update({
                        "keyword": r.ad_group_criterion.keyword.text,
                    })
                
                rows.append(row)
        
        return pd.DataFrame(rows)
    
    except Exception as e:
        print(f"Google Ads API エラー: {e}")
        return pd.DataFrame()


def fetch_ads_campaign_daily(start: str, end: str) -> pd.DataFrame:
    """
    キャンペーン日別データを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: キャンペーン日別データ
    """
    customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise ValueError("GOOGLE_ADS_CUSTOMER_ID 環境変数が設定されていません")
    
    gaql = GAQL_CAMPAIGN.format(start=start, end=end)
    return fetch_gaql(customer_id, gaql)


def fetch_ads_adgroup_daily(start: str, end: str) -> pd.DataFrame:
    """
    広告グループ日別データを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: 広告グループ日別データ
    """
    customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise ValueError("GOOGLE_ADS_CUSTOMER_ID 環境変数が設定されていません")
    
    gaql = GAQL_ADGROUP.format(start=start, end=end)
    return fetch_gaql(customer_id, gaql)


def fetch_ads_keyword_daily(start: str, end: str) -> pd.DataFrame:
    """
    キーワード日別データを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: キーワード日別データ
    """
    customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
    if not customer_id:
        raise ValueError("GOOGLE_ADS_CUSTOMER_ID 環境変数が設定されていません")
    
    gaql = GAQL_KEYWORD.format(start=start, end=end)
    return fetch_gaql(customer_id, gaql)


def fetch_ads_all_daily(start: str, end: str) -> dict:
    """
    全Google Adsデータを日別で取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        dict: 各レベルのデータフレーム
    """
    return {
        "campaign": fetch_ads_campaign_daily(start, end),
        "adgroup": fetch_ads_adgroup_daily(start, end),
        "keyword": fetch_ads_keyword_daily(start, end),
    }
