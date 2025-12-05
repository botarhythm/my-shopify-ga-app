#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
統合データ分析スクリプト
4つのプラットフォームのデータを統合して分析
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

# 環境変数を読み込み
load_dotenv()

def load_all_data(start_date: date, end_date: date):
    """
    全プラットフォームのデータを取得
    
    Args:
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        dict: 各プラットフォームのデータ
    """
    print("=== 統合データ取得開始 ===")
    
    data = {}
    
    # 1. Shopifyデータ取得
    print("1. Shopifyデータ取得中...")
    try:
        from src.connectors.shopify import fetch_orders_incremental
        
        start_iso = f"{start_date}T00:00:00Z"
        orders_df = fetch_orders_incremental(start_iso)
        
        if not orders_df.empty:
            # 指定期間のデータのみフィルタリング
            filtered_orders = orders_df[
                (orders_df['date'] >= start_date) & 
                (orders_df['date'] <= end_date)
            ]
            
            # 注文単位で集計
            order_summary = filtered_orders.groupby('order_id').agg({
                'order_total': 'first',
                'created_at': 'first',
                'financial_status': 'first',
                'subtotal_price': 'first',
                'total_discounts': 'first',
                'total_tax': 'first',
                'total_tip_received': 'first',
                'shipping_price': 'first'
            }).reset_index()
            
            data['shopify'] = order_summary
            print(f"   ✅ Shopify: {len(order_summary)}件の注文")
        else:
            data['shopify'] = pd.DataFrame()
            print("   ❌ Shopify: データなし")
            
    except Exception as e:
        print(f"   ❌ Shopify エラー: {e}")
        data['shopify'] = pd.DataFrame()
    
    # 2. Squareデータ取得
    print("2. Squareデータ取得中...")
    try:
        from src.connectors.square import fetch_payments
        
        payments_df = fetch_payments(start_date, end_date)
        
        if not payments_df.empty:
            data['square'] = payments_df
            print(f"   ✅ Square: {len(payments_df)}件の支払い")
        else:
            data['square'] = pd.DataFrame()
            print("   ❌ Square: データなし")
            
    except Exception as e:
        print(f"   ❌ Square エラー: {e}")
        data['square'] = pd.DataFrame()
    
    # 3. GA4データ取得
    print("3. GA4データ取得中...")
    try:
        from fix_ga4_api_error import fetch_ga4_compatible
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        result = fetch_ga4_compatible(start_str, end_str)
        
        if not result.empty:
            data['ga4'] = result
            print(f"   ✅ GA4: {len(result)}件のデータ")
        else:
            data['ga4'] = pd.DataFrame()
            print("   ❌ GA4: データなし")
            
    except Exception as e:
        print(f"   ❌ GA4 エラー: {e}")
        data['ga4'] = pd.DataFrame()
    
    # 4. Google Adsデータ取得
    print("4. Google Adsデータ取得中...")
    try:
        from src.ads.google_ads_client import create_google_ads_client
        from src.ads.fetch_ads import fetch_campaign_data
        
        client = create_google_ads_client()
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        campaign_data = fetch_campaign_data(client, start_str, end_str)
        
        if not campaign_data.empty:
            data['google_ads'] = campaign_data
            print(f"   ✅ Google Ads: {len(campaign_data)}件のキャンペーン")
        else:
            data['google_ads'] = pd.DataFrame()
            print("   ❌ Google Ads: データなし")
            
    except Exception as e:
        print(f"   ❌ Google Ads エラー: {e}")
        data['google_ads'] = pd.DataFrame()
    
    print("=== データ取得完了 ===")
    return data

def calculate_kpi_summary(data: dict):
    """
    KPIサマリーを計算
    
    Args:
        data: 各プラットフォームのデータ
    
    Returns:
        dict: KPIサマリー
    """
    kpi = {}
    
    # Shopify KPI
    if not data['shopify'].empty:
        shopify_df = data['shopify']
        kpi['shopify'] = {
            'total_orders': len(shopify_df),
            'total_revenue': shopify_df['order_total'].sum(),
            'avg_order_value': shopify_df['order_total'].mean(),
            'total_discounts': shopify_df['total_discounts'].sum(),
            'total_tax': shopify_df['total_tax'].sum(),
            'total_tips': shopify_df['total_tip_received'].sum(),
            'total_shipping': shopify_df['shipping_price'].sum()
        }
    else:
        kpi['shopify'] = {
            'total_orders': 0,
            'total_revenue': 0,
            'avg_order_value': 0,
            'total_discounts': 0,
            'total_tax': 0,
            'total_tips': 0,
            'total_shipping': 0
        }
    
    # Square KPI
    if not data['square'].empty:
        square_df = data['square']
        kpi['square'] = {
            'total_payments': len(square_df),
            'total_revenue': square_df['amount'].sum(),
            'avg_payment_value': square_df['amount'].mean(),
            'total_processing_fees': square_df['processing_fee'].sum() if 'processing_fee' in square_df.columns else 0
        }
    else:
        kpi['square'] = {
            'total_payments': 0,
            'total_revenue': 0,
            'avg_payment_value': 0,
            'total_processing_fees': 0
        }
    
    # GA4 KPI
    if not data['ga4'].empty:
        ga4_df = data['ga4']
        kpi['ga4'] = {
            'total_sessions': ga4_df['sessions'].sum(),
            'total_users': ga4_df['users'].sum(),
            'total_revenue': ga4_df['revenue'].sum(),
            'total_purchases': ga4_df['purchases'].sum(),
            'avg_session_duration': ga4_df['session_duration'].mean() if 'session_duration' in ga4_df.columns else 0
        }
    else:
        kpi['ga4'] = {
            'total_sessions': 0,
            'total_users': 0,
            'total_revenue': 0,
            'total_purchases': 0,
            'avg_session_duration': 0
        }
    
    # Google Ads KPI
    if not data['google_ads'].empty:
        ads_df = data['google_ads']
        kpi['google_ads'] = {
            'total_campaigns': len(ads_df),
            'total_impressions': ads_df['impressions'].sum(),
            'total_clicks': ads_df['clicks'].sum(),
            'total_cost': ads_df['cost_micros'].sum() / 1000000,  # マイクロ単位から円に変換
            'total_conversions': ads_df['conversions'].sum(),
            'avg_ctr': ads_df['ctr'].mean(),
            'avg_cpc': ads_df['average_cpc'].sum() / 1000000  # マイクロ単位から円に変換
        }
    else:
        kpi['google_ads'] = {
            'total_campaigns': 0,
            'total_impressions': 0,
            'total_clicks': 0,
            'total_cost': 0,
            'total_conversions': 0,
            'avg_ctr': 0,
            'avg_cpc': 0
        }
    
    # 統合KPI
    total_revenue = kpi['shopify']['total_revenue'] + kpi['square']['total_revenue']
    total_cost = kpi['google_ads']['total_cost']
    
    kpi['integrated'] = {
        'total_revenue': total_revenue,
        'total_cost': total_cost,
        'roas': total_revenue / total_cost if total_cost > 0 else 0,
        'total_orders': kpi['shopify']['total_orders'] + kpi['square']['total_payments'],
        'total_sessions': kpi['ga4']['total_sessions'],
        'conversion_rate': (kpi['ga4']['total_purchases'] / kpi['ga4']['total_sessions'] * 100) if kpi['ga4']['total_sessions'] > 0 else 0
    }
    
    return kpi

def generate_analysis_report(data: dict, kpi: dict, start_date: date, end_date: date):
    """
    分析レポートを生成
    
    Args:
        data: 各プラットフォームのデータ
        kpi: KPIサマリー
        start_date: 開始日
        end_date: 終了日
    """
    print("\n" + "="*60)
    print("📊 統合データ分析レポート")
    print("="*60)
    print(f"期間: {start_date} 〜 {end_date}")
    print(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 売上サマリー
    print("\n💰 売上サマリー")
    print("-" * 40)
    print(f"Shopify売上:     ¥{kpi['shopify']['total_revenue']:>12,.0f}")
    print(f"Square売上:      ¥{kpi['square']['total_revenue']:>12,.0f}")
    print(f"GA4売上:         ¥{kpi['ga4']['total_revenue']:>12,.0f}")
    print(f"総売上:          ¥{kpi['integrated']['total_revenue']:>12,.0f}")
    
    # 2. 注文・支払いサマリー
    print("\n📦 注文・支払いサマリー")
    print("-" * 40)
    print(f"Shopify注文数:    {kpi['shopify']['total_orders']:>12,}件")
    print(f"Square支払い数:   {kpi['square']['total_payments']:>12,}件")
    print(f"総取引数:         {kpi['integrated']['total_orders']:>12,}件")
    
    if kpi['shopify']['total_orders'] > 0:
        print(f"Shopify平均注文額: ¥{kpi['shopify']['avg_order_value']:>10,.0f}")
    if kpi['square']['total_payments'] > 0:
        print(f"Square平均支払い額: ¥{kpi['square']['avg_payment_value']:>10,.0f}")
    
    # 3. トラフィックサマリー
    print("\n🌐 トラフィックサマリー")
    print("-" * 40)
    print(f"GA4セッション数:  {kpi['ga4']['total_sessions']:>12,}")
    print(f"GA4ユーザー数:    {kpi['ga4']['total_users']:>12,}")
    print(f"コンバージョン率:  {kpi['integrated']['conversion_rate']:>11.2f}%")
    
    # 4. 広告サマリー
    print("\n📈 広告サマリー")
    print("-" * 40)
    print(f"Google Ads費用:    ¥{kpi['google_ads']['total_cost']:>12,.0f}")
    print(f"インプレッション数: {kpi['google_ads']['total_impressions']:>12,}")
    print(f"クリック数:        {kpi['google_ads']['total_clicks']:>12,}")
    print(f"平均CTR:          {kpi['google_ads']['avg_ctr']:>11.2f}%")
    print(f"平均CPC:          ¥{kpi['google_ads']['avg_cpc']:>10,.0f}")
    
    # 5. ROAS分析
    print("\n🎯 ROAS分析")
    print("-" * 40)
    print(f"総売上:           ¥{kpi['integrated']['total_revenue']:>12,.0f}")
    print(f"広告費用:         ¥{kpi['google_ads']['total_cost']:>12,.0f}")
    print(f"ROAS:             {kpi['integrated']['roas']:>11.2f}")
    
    # 6. データ品質チェック
    print("\n🔍 データ品質チェック")
    print("-" * 40)
    
    # Shopifyデータ品質
    if not data['shopify'].empty:
        shopify_df = data['shopify']
        print(f"Shopify: {len(shopify_df)}件の注文")
        print(f"  - 売上範囲: ¥{shopify_df['order_total'].min():,.0f} 〜 ¥{shopify_df['order_total'].max():,.0f}")
        print(f"  - 平均注文額: ¥{shopify_df['order_total'].mean():,.0f}")
        print(f"  - 標準偏差: ¥{shopify_df['order_total'].std():,.0f}")
    else:
        print("Shopify: データなし")
    
    # Squareデータ品質
    if not data['square'].empty:
        square_df = data['square']
        print(f"Square: {len(square_df)}件の支払い")
        print(f"  - 売上範囲: ¥{square_df['amount'].min():,.0f} 〜 ¥{square_df['amount'].max():,.0f}")
        print(f"  - 平均支払い額: ¥{square_df['amount'].mean():,.0f}")
        print(f"  - 標準偏差: ¥{square_df['amount'].std():,.0f}")
    else:
        print("Square: データなし")
    
    # GA4データ品質
    if not data['ga4'].empty:
        ga4_df = data['ga4']
        print(f"GA4: {len(ga4_df)}件のデータ")
        print(f"  - セッション範囲: {ga4_df['sessions'].min():,.0f} 〜 {ga4_df['sessions'].max():,.0f}")
        print(f"  - 売上範囲: ¥{ga4_df['revenue'].min():,.0f} 〜 ¥{ga4_df['revenue'].max():,.0f}")
    else:
        print("GA4: データなし")
    
    print("\n" + "="*60)

def main():
    """メイン実行"""
    print("統合データ分析開始")
    
    # 2025年8月のデータを分析
    start_date = date(2025, 8, 1)
    end_date = date(2025, 8, 31)
    
    # データ取得
    data = load_all_data(start_date, end_date)
    
    # KPI計算
    kpi = calculate_kpi_summary(data)
    
    # 分析レポート生成
    generate_analysis_report(data, kpi, start_date, end_date)
    
    # 結果をファイルに保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"integrated_analysis_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"# 統合データ分析レポート\n\n")
        f.write(f"**期間**: {start_date} 〜 {end_date}\n")
        f.write(f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## 売上サマリー\n\n")
        f.write(f"- **Shopify売上**: ¥{kpi['shopify']['total_revenue']:,}\n")
        f.write(f"- **Square売上**: ¥{kpi['square']['total_revenue']:,}\n")
        f.write(f"- **GA4売上**: ¥{kpi['ga4']['total_revenue']:,}\n")
        f.write(f"- **総売上**: ¥{kpi['integrated']['total_revenue']:,}\n\n")
        
        f.write("## 取引サマリー\n\n")
        f.write(f"- **Shopify注文数**: {kpi['shopify']['total_orders']:,}件\n")
        f.write(f"- **Square支払い数**: {kpi['square']['total_payments']:,}件\n")
        f.write(f"- **総取引数**: {kpi['integrated']['total_orders']:,}件\n\n")
        
        f.write("## トラフィックサマリー\n\n")
        f.write(f"- **GA4セッション数**: {kpi['ga4']['total_sessions']:,}\n")
        f.write(f"- **GA4ユーザー数**: {kpi['ga4']['total_users']:,}\n")
        f.write(f"- **コンバージョン率**: {kpi['integrated']['conversion_rate']:.2f}%\n\n")
        
        f.write("## 広告サマリー\n\n")
        f.write(f"- **Google Ads費用**: ¥{kpi['google_ads']['total_cost']:,}\n")
        f.write(f"- **インプレッション数**: {kpi['google_ads']['total_impressions']:,}\n")
        f.write(f"- **クリック数**: {kpi['google_ads']['total_clicks']:,}\n")
        f.write(f"- **平均CTR**: {kpi['google_ads']['avg_ctr']:.2f}%\n")
        f.write(f"- **平均CPC**: ¥{kpi['google_ads']['avg_cpc']:.0f}\n\n")
        
        f.write("## ROAS分析\n\n")
        f.write(f"- **ROAS**: {kpi['integrated']['roas']:.2f}\n")
        f.write(f"- **総売上**: ¥{kpi['integrated']['total_revenue']:,}\n")
        f.write(f"- **広告費用**: ¥{kpi['google_ads']['total_cost']:,}\n")
    
    print(f"\n📄 分析レポートを保存しました: {report_file}")
    print("統合データ分析完了")

if __name__ == "__main__":
    main()