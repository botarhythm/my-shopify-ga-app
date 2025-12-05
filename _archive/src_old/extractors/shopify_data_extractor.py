#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify Admin API データ取得スクリプト
注文データと商品データを取得し、CSVファイルとして出力します。

必要なライブラリのインストール:
pip install shopify-python-api requests pandas python-dotenv

または
pip install -r requirements.txt
"""

import os
import requests
import pandas as pd
from datetime import datetime
import json
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

# Shopify設定
SHOPIFY_API_TOKEN = os.getenv('SHOPIFY_API_TOKEN')
SHOPIFY_STORE_URL = 'botarhythm.com'  # ストアURL
SHOPIFY_API_VERSION = '2024-01'  # APIバージョン

def get_shopify_headers():
    """Shopify API用のヘッダーを取得します。"""
    if not SHOPIFY_API_TOKEN:
        raise ValueError("SHOPIFY_API_TOKEN環境変数が設定されていません。")
    
    return {
        'X-Shopify-Access-Token': SHOPIFY_API_TOKEN,
        'Content-Type': 'application/json'
    }

def get_shopify_orders():
    """Shopifyから注文データを取得します。"""
    print("注文データを取得中...")
    
    # 過去30日の期間を設定
    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Shopify API用の日時フォーマット（ISO 8601）
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S-04:00')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S-04:00')
    
    print(f"取得期間: {start_date.strftime('%Y年%m月%d日')} 〜 {end_date.strftime('%Y年%m月%d日')}")
    
    headers = get_shopify_headers()
    orders = []
    page_info = None
    
    while True:
        # APIエンドポイントの構築
        url = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
        params = {
            'status': 'any',  # 全ての注文ステータス
            'limit': 250,     # 最大取得件数
            'created_at_min': start_date_str,  # 開始日時
            'created_at_max': end_date_str,    # 終了日時
        }
        
        if page_info:
            params['page_info'] = page_info
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            orders.extend(data.get('orders', []))
            
            # ページネーション処理
            link_header = response.headers.get('Link', '')
            if 'rel="next"' in link_header:
                # 次のページのpage_infoを抽出
                next_link = [link for link in link_header.split(', ') if 'rel="next"' in link]
                if next_link:
                    page_info = next_link[0].split('page_info=')[1].split('>')[0]
                else:
                    break
            else:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"注文データ取得中にエラーが発生しました: {e}")
            break
    
    print(f"取得した注文数: {len(orders)}")
    return orders

def get_shopify_products():
    """Shopifyから商品データを取得します。"""
    print("商品データを取得中...")
    
    headers = get_shopify_headers()
    products = []
    page_info = None
    
    while True:
        # APIエンドポイントの構築
        url = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/products.json"
        params = {
            'limit': 250,  # 最大取得件数
        }
        
        if page_info:
            params['page_info'] = page_info
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            products.extend(data.get('products', []))
            
            # ページネーション処理
            link_header = response.headers.get('Link', '')
            if 'rel="next"' in link_header:
                # 次のページのpage_infoを抽出
                next_link = [link for link in link_header.split(', ') if 'rel="next"' in link]
                if next_link:
                    page_info = next_link[0].split('page_info=')[1].split('>')[0]
                else:
                    break
            else:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"商品データ取得中にエラーが発生しました: {e}")
            break
    
    print(f"取得した商品数: {len(products)}")
    return products

def process_orders_data(orders):
    """注文データを処理してDataFrameに変換します。"""
    if not orders:
        return pd.DataFrame()
    
    processed_orders = []
    
    for order in orders:
        # 基本注文情報
        order_data = {
            'id': order.get('id'),
            'order_number': order.get('order_number'),
            'created_at': order.get('created_at'),
            'total_price': order.get('total_price'),
            'subtotal_price': order.get('subtotal_price'),
            'total_tax': order.get('total_tax'),
            'currency': order.get('currency'),
            'financial_status': order.get('financial_status'),
            'fulfillment_status': order.get('fulfillment_status'),
            'customer_id': order.get('customer', {}).get('id') if order.get('customer') else None,
            'customer_email': order.get('customer', {}).get('email') if order.get('customer') else None,
            'customer_first_name': order.get('customer', {}).get('first_name') if order.get('customer') else None,
            'customer_last_name': order.get('customer', {}).get('last_name') if order.get('customer') else None,
            'shipping_address_country': order.get('shipping_address', {}).get('country') if order.get('shipping_address') else None,
            'billing_address_country': order.get('billing_address', {}).get('country') if order.get('billing_address') else None,
        }
        
        # 商品情報を追加
        line_items = order.get('line_items', [])
        if line_items:
            for item in line_items:
                item_data = order_data.copy()
                item_data.update({
                    'product_id': item.get('product_id'),
                    'variant_id': item.get('variant_id'),
                    'product_title': item.get('title'),
                    'variant_title': item.get('variant_title'),
                    'quantity': item.get('quantity'),
                    'price': item.get('price'),
                    'total_discount': item.get('total_discount'),
                })
                processed_orders.append(item_data)
        else:
            processed_orders.append(order_data)
    
    return pd.DataFrame(processed_orders)

def process_products_data(products):
    """商品データを処理してDataFrameに変換します。"""
    if not products:
        return pd.DataFrame()
    
    processed_products = []
    
    for product in products:
        product_data = {
            'id': product.get('id'),
            'title': product.get('title'),
            'body_html': product.get('body_html'),
            'vendor': product.get('vendor'),
            'product_type': product.get('product_type'),
            'tags': product.get('tags'),
            'status': product.get('status'),
            'created_at': product.get('created_at'),
            'updated_at': product.get('updated_at'),
            'published_at': product.get('published_at'),
            'template_suffix': product.get('template_suffix'),
            'admin_graphql_api_id': product.get('admin_graphql_api_id'),
        }
        
        # バリアント情報を追加
        variants = product.get('variants', [])
        if variants:
            for variant in variants:
                variant_data = product_data.copy()
                variant_data.update({
                    'variant_id': variant.get('id'),
                    'variant_title': variant.get('title'),
                    'variant_price': variant.get('price'),
                    'variant_sku': variant.get('sku'),
                    'variant_barcode': variant.get('barcode'),
                    'variant_weight': variant.get('weight'),
                    'variant_weight_unit': variant.get('weight_unit'),
                    'variant_inventory_quantity': variant.get('inventory_quantity'),
                    'variant_inventory_policy': variant.get('inventory_policy'),
                    'variant_fulfillment_service': variant.get('fulfillment_service'),
                })
                processed_products.append(variant_data)
        else:
            processed_products.append(product_data)
    
    return pd.DataFrame(processed_products)

def main():
    """メイン処理を実行します。"""
    print("Shopify Admin API データ取得を開始します...")
    
    try:
        # 環境変数の確認
        if not SHOPIFY_API_TOKEN:
            print("エラー: SHOPIFY_API_TOKEN環境変数が設定されていません。")
            print("以下のコマンドで環境変数を設定してください:")
            print("set SHOPIFY_API_TOKEN=your_api_token_here")
            return
        
        print(f"ストアURL: {SHOPIFY_STORE_URL}")
        print(f"APIバージョン: {SHOPIFY_API_VERSION}")
        
        # データ取得
        orders = get_shopify_orders()
        products = get_shopify_products()
        
        if not orders and not products:
            print("データの取得に失敗しました。")
            return
        
        # データ処理
        print("\nデータを処理中...")
        
        if orders:
            orders_df = process_orders_data(orders)
            if not orders_df.empty:
                # 注文データをCSV出力
                orders_filename = f"shopify_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                orders_df.to_csv(orders_filename, index=False, encoding='utf-8-sig')
                print(f"注文データを {orders_filename} に保存しました。")
                print(f"注文データ件数: {len(orders_df)}")
                print("\n注文データのプレビュー:")
                print(orders_df.head())
        
        if products:
            products_df = process_products_data(products)
            if not products_df.empty:
                # 商品データをCSV出力
                products_filename = f"shopify_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                products_df.to_csv(products_filename, index=False, encoding='utf-8-sig')
                print(f"\n商品データを {products_filename} に保存しました。")
                print(f"商品データ件数: {len(products_df)}")
                print("\n商品データのプレビュー:")
                print(products_df.head())
        
        print("\nデータ取得が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

