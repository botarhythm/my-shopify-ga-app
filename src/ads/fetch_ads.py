#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads データ取得モジュール
キャンペーンデータの取得と処理
"""

import os
import pandas as pd
from datetime import datetime, date
from typing import Optional
from google.ads.googleads.errors import GoogleAdsException

def fetch_campaign_data(client, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Google Adsキャンペーンデータを取得
    
    Args:
        client: Google Ads API クライアント
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: キャンペーンデータ
    """
    try:
        from google.ads.googleads.client import GoogleAdsClient
        
        # Google Ads Serviceを取得
        ga_service = client.get_service("GoogleAdsService")
        
        # 顧客IDを取得
        customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
        if not customer_id:
            raise ValueError("GOOGLE_ADS_CUSTOMER_ID 環境変数が設定されていません")
        
        # GAQLクエリを構築
        query = f"""
            SELECT 
                segments.date,
                campaign.id,
                campaign.name,
                campaign.status,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.cost_micros,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value
            FROM campaign 
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND campaign.status = 'ENABLED'
            ORDER BY segments.date DESC
        """
        
        # データを取得
        response = ga_service.search(customer_id=customer_id, query=query)
        
        # 結果をDataFrameに変換
        rows = []
        for row in response:
            row_data = {
                'date': row.segments.date,
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'campaign_status': row.campaign.status,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'ctr': row.metrics.ctr,
                'cost_micros': row.metrics.cost_micros,
                'average_cpc': row.metrics.average_cpc,
                'conversions': row.metrics.conversions,
                'conversions_value': row.metrics.conversions_value
            }
            rows.append(row_data)
        
        df = pd.DataFrame(rows)
        
        # データ型を変換
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce')
            df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce')
            df['ctr'] = pd.to_numeric(df['ctr'], errors='coerce')
            df['cost_micros'] = pd.to_numeric(df['cost_micros'], errors='coerce')
            df['average_cpc'] = pd.to_numeric(df['average_cpc'], errors='coerce')
            df['conversions'] = pd.to_numeric(df['conversions'], errors='coerce')
            df['conversions_value'] = pd.to_numeric(df['conversions_value'], errors='coerce')
        
        return df
        
    except GoogleAdsException as e:
        print(f"Google Ads API エラー: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Google Ads データ取得エラー: {e}")
        return pd.DataFrame()

def fetch_ad_group_data(client, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Google Ads広告グループデータを取得
    
    Args:
        client: Google Ads API クライアント
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: 広告グループデータ
    """
    try:
        from google.ads.googleads.client import GoogleAdsClient
        
        # Google Ads Serviceを取得
        ga_service = client.get_service("GoogleAdsService")
        
        # 顧客IDを取得
        customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
        if not customer_id:
            raise ValueError("GOOGLE_ADS_CUSTOMER_ID 環境変数が設定されていません")
        
        # GAQLクエリを構築
        query = f"""
            SELECT 
                segments.date,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group.status,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.cost_micros,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value
            FROM ad_group 
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND ad_group.status = 'ENABLED'
            ORDER BY segments.date DESC
        """
        
        # データを取得
        response = ga_service.search(customer_id=customer_id, query=query)
        
        # 結果をDataFrameに変換
        rows = []
        for row in response:
            row_data = {
                'date': row.segments.date,
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'ad_group_id': row.ad_group.id,
                'ad_group_name': row.ad_group.name,
                'ad_group_status': row.ad_group.status,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'ctr': row.metrics.ctr,
                'cost_micros': row.metrics.cost_micros,
                'average_cpc': row.metrics.average_cpc,
                'conversions': row.metrics.conversions,
                'conversions_value': row.metrics.conversions_value
            }
            rows.append(row_data)
        
        df = pd.DataFrame(rows)
        
        # データ型を変換
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce')
            df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce')
            df['ctr'] = pd.to_numeric(df['ctr'], errors='coerce')
            df['cost_micros'] = pd.to_numeric(df['cost_micros'], errors='coerce')
            df['average_cpc'] = pd.to_numeric(df['average_cpc'], errors='coerce')
            df['conversions'] = pd.to_numeric(df['conversions'], errors='coerce')
            df['conversions_value'] = pd.to_numeric(df['conversions_value'], errors='coerce')
        
        return df
        
    except GoogleAdsException as e:
        print(f"Google Ads API エラー: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Google Ads データ取得エラー: {e}")
        return pd.DataFrame()

def calculate_roas(cost: float, revenue: float) -> float:
    """
    ROAS (Return on Ad Spend) を計算
    
    Args:
        cost: 広告費用
        revenue: 売上
    
    Returns:
        float: ROAS値
    """
    if cost > 0:
        return revenue / cost
    return 0.0

def calculate_cpa(cost: float, conversions: float) -> float:
    """
    CPA (Cost Per Acquisition) を計算
    
    Args:
        cost: 広告費用
        conversions: コンバージョン数
    
    Returns:
        float: CPA値
    """
    if conversions > 0:
        return cost / conversions
    return 0.0