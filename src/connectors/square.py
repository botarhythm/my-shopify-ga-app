"""
Square データ取得コネクタ
Paymentsデータの増分取得
"""
import os
import pandas as pd
from square.client import Client
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential


def _get_client() -> Client:
    """Square クライアントを取得"""
    access_token = os.getenv("SQUARE_ACCESS_TOKEN")
    if not access_token:
        raise ValueError("SQUARE_ACCESS_TOKEN 環境変数が設定されていません")
    return Client(access_token=access_token)


def _get_location_id() -> str:
    """Square ロケーションIDを取得"""
    location_id = os.getenv("SQUARE_LOCATION_ID")
    if not location_id:
        raise ValueError("SQUARE_LOCATION_ID 環境変数が設定されていません")
    return location_id


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_payments(start: str, end: str) -> pd.DataFrame:
    """
    Square Paymentsデータを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: Paymentsデータ
    """
    client = _get_client()
    location_id = _get_location_id()
    
    cursor = None
    rows = []
    
    while True:
        try:
            resp = client.payments.list_payments(
                begin_time=f"{start}T00:00:00Z",
                end_time=f"{end}T23:59:59Z",
                location_id=location_id,
                cursor=cursor,
                sort_order="ASC"
            )
            
            if not resp.is_success():
                print(f"Square API エラー: {resp.errors}")
                break
                
            body = resp.body
            
            for payment in body.get("payments", []):
                # 金額の変換（通貨最小単位 → 通貨単位）
                def _to_major_units(amount_minor: int, currency_code: str) -> float:
                    # Squareのamountは最小単位。JPYは小数なし（そのまま）。その他は100で割る
                    if currency_code == "JPY":
                        return float(amount_minor)
                    return float(amount_minor) / 100.0

                row = {
                    "date": payment["created_at"][:10],  # 日付部分のみ抽出
                    "payment_id": payment["id"],
                    "created_at": payment["created_at"][:10],  # 日付部分のみ抽出
                    "amount": _to_major_units(payment["amount_money"]["amount"], payment["amount_money"]["currency"]),
                    "currency": payment["amount_money"]["currency"],
                    "card_brand": payment.get("card_details", {}).get("card", {}).get("card_brand"),
                    "status": payment.get("status"),
                    "receipt_number": payment.get("receipt_number"),
                    "order_id": payment.get("order_id"),
                    "location_id": payment.get("location_id"),
                    "merchant_id": payment.get("merchant_id"),
                    "card_type": None,
                    "card_fingerprint": None,
                    "entry_method": None,
                    "processing_fee": None,
                }
                
                # 追加の支払い詳細情報
                if payment.get("card_details"):
                    card_details = payment["card_details"]
                    row.update({
                        "card_type": card_details.get("card", {}).get("card_type"),
                        "card_fingerprint": card_details.get("card", {}).get("fingerprint"),
                        "entry_method": card_details.get("entry_method"),
                    })
                
                # 手数料情報（配列で返る場合があるため合算）
                if payment.get("processing_fee"):
                    fees = payment["processing_fee"]
                    total_fee_minor = 0
                    if isinstance(fees, list):
                        for fee in fees:
                            amt = fee.get("amount_money", {}).get("amount", 0)
                            total_fee_minor += int(amt)
                        row["processing_fee"] = _to_major_units(total_fee_minor, payment["amount_money"]["currency"])
                    elif isinstance(fees, dict):
                        amt = fees.get("amount_money", {}).get("amount", 0)
                        row["processing_fee"] = _to_major_units(int(amt), payment["amount_money"]["currency"])
                
                rows.append(row)
            
            cursor = body.get("cursor")
            if not cursor:
                break
                
        except Exception as e:
            print(f"Square API エラー: {e}")
            break
    
    return pd.DataFrame(rows)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_refunds(start: str, end: str) -> pd.DataFrame:
    """
    Square Refundsデータを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: Refundsデータ
    """
    client = _get_client()
    location_id = _get_location_id()
    
    cursor = None
    rows = []
    
    while True:
        try:
            # Square API v2ではlist_refundsは存在しないため、paymentsからrefundsを取得
            resp = client.payments.list_payments(
                begin_time=f"{start}T00:00:00Z",
                end_time=f"{end}T23:59:59Z",
                location_id=location_id,
                cursor=cursor,
                sort_order="ASC"
            )
            
            if not resp.is_success():
                print(f"Square API エラー: {resp.errors}")
                break
                
            body = resp.body
            
            for payment in body.get("payments", []):
                # 支払いに関連する返金を取得
                if payment.get("refunds"):
                    for refund in payment["refunds"]:
                        row = {
                            "refund_id": refund["id"],
                            "payment_id": payment["id"],
                            "created_at": refund["created_at"][:10],
                            "amount": refund["amount_money"]["amount"] / 100,
                            "currency": refund["amount_money"]["currency"],
                            "status": refund.get("status"),
                            "reason": refund.get("reason"),
                            "location_id": payment.get("location_id"),
                            "merchant_id": payment.get("merchant_id"),
                        }
                        rows.append(row)
            
            cursor = body.get("cursor")
            if not cursor:
                break
                
        except Exception as e:
            print(f"Square API エラー: {e}")
            break
    
    return pd.DataFrame(rows)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_orders(start: str, end: str) -> pd.DataFrame:
    """
    Square Ordersデータを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame: Ordersデータ
    """
    client = _get_client()
    location_id = _get_location_id()
    
    cursor = None
    rows = []
    
    while True:
        try:
            resp = client.orders.search_orders(
                query={
                    "filter": {
                        "location_filter": {
                            "location_ids": [location_id]
                        },
                        "date_time_filter": {
                            "created_at": {
                                "start_at": f"{start}T00:00:00Z",
                                "end_at": f"{end}T23:59:59Z"
                            }
                        }
                    }
                },
                cursor=cursor
            )
            
            if not resp.is_success():
                print(f"Square API エラー: {resp.errors}")
                break
                
            body = resp.body
            
            for order in body.get("orders", []):
                row = {
                    "order_id": order["id"],
                    "created_at": order["created_at"][:10],
                    "updated_at": order["updated_at"][:10],
                    "state": order.get("state"),
                    "total_money": order.get("total_money", {}).get("amount", 0) / 100,
                    "total_tax_money": order.get("total_tax_money", {}).get("amount", 0) / 100,
                    "total_discount_money": order.get("total_discount_money", {}).get("amount", 0) / 100,
                    "total_service_charge_money": order.get("total_service_charge_money", {}).get("amount", 0) / 100,
                    "location_id": order.get("location_id"),
                    "customer_id": order.get("customer_id"),
                }
                rows.append(row)
            
            cursor = body.get("cursor")
            if not cursor:
                break
                
        except Exception as e:
            print(f"Square API エラー: {e}")
            break
    
    return pd.DataFrame(rows)


def fetch_square_all(start: str, end: str) -> dict:
    """
    全Squareデータを取得
    
    Args:
        start: 開始日 (YYYY-MM-DD)
        end: 終了日 (YYYY-MM-DD)
    
    Returns:
        dict: 各データタイプのDataFrame
    """
    return {
        "payments": fetch_payments(start, end),
        "refunds": fetch_refunds(start, end),
        "orders": fetch_orders(start, end),
    }
