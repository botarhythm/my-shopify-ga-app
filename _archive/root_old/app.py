# app.py
# ----------------------------------------------------------------------
# Shopify × GA4 ダッシュボード（本番データのみでデバッグ）
# 単一ファイル版（Cursorでそのまま貼り付け可）
# 目的:
#  - モック完全排除（MOCK* 変数があれば即停止）
#  - 必須環境変数チェック → 不足あればUIで案内して停止
#  - ヘルスチェック: 本番APIに軽量アクセスで接続健全性を確認
#  - ワンクリック更新: 既存の抽出・分析パイプラインを呼び出し、完了時に「最終更新」を記録
#  - エラーはUIで日本語表示（traceはエキスパンダに格納）
# ----------------------------------------------------------------------

import os
import json
import time
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from functools import wraps

import streamlit as st
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

# =========================
# 必須環境変数の定義と検証
# =========================
REQUIRED_ENVS = [
    # ---- Shopify ----
    "SHOPIFY_SHOP_URL",
    "SHOPIFY_ACCESS_TOKEN",
    # ---- GA4 ----
    "GA4_PROPERTY_ID",
    "GOOGLE_APPLICATION_CREDENTIALS",
    # ---- Square ----
    "SQUARE_ACCESS_TOKEN",
    "SQUARE_LOCATION_ID",
    # ---- Google Ads ----
    "GOOGLE_ADS_CLIENT_ID",
    "GOOGLE_ADS_CLIENT_SECRET",
    "GOOGLE_ADS_REFRESH_TOKEN",
    "GOOGLE_ADS_DEVELOPER_TOKEN",
    "GOOGLE_ADS_CUSTOMER_ID",
    # ---- DB ----
    "DUCKDB_PATH",
]

@dataclass
class EnvCheckResult:
    missing: List[str]
    present: List[str]

def validate_required_envs(required=REQUIRED_ENVS) -> EnvCheckResult:
    missing, present = [], []
    for k in required:
        if os.getenv(k):
            present.append(k)
        else:
            missing.append(k)
    return EnvCheckResult(missing=missing, present=present)

def forbid_mock_usage():
    """
    モック利用の痕跡を技術的にブロック。
    - 'MOCK' を含む環境変数があれば即停止
    """
    forbidden_envs = [k for k in os.environ.keys() if "MOCK" in k.upper()]
    if forbidden_envs:
        raise RuntimeError(
            f"MOCK関連の環境変数が検出されました: {forbidden_envs}\n"
            "モックを完全排除する方針のため、すべて削除してから再起動してください。"
        )

# =========================
# 例外をUIで可視化する安全ラッパ
# =========================
def safe_action(label_when_running: str = "処理中..."):
    """
    UIボタン等で呼ぶ関数に付与。例外→UIに日本語エラー＋詳細（折り畳み）を表示。
    """
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                with st.spinner(label_when_running):
                    return fn(*args, **kwargs)
            except Exception as e:
                st.error("⚠️ エラーが発生しました。設定やネットワークをご確認ください。")
                with st.expander("エラー詳細（開発者向け）"):
                    import traceback
                    st.code("".join(traceback.format_exc()))
                return None
        return wrapper
    return deco

# =========================
# ヘルスチェック（本番APIへの軽量呼び出し）
# =========================
@dataclass
class HealthStatus:
    shopify_ok: bool
    ga4_ok: bool
    square_ok: bool
    google_ads_ok: bool
    detail: Dict[str, str]

def check_shopify() -> Tuple[bool, str]:
    """
    Shopify管理APIの軽量エンドポイントへアクセスし、認証/到達性を確認
    """
    try:
        from src.connectors.shopify import _get_base_url, _get_headers
        import requests
        
        base_url = _get_base_url()
        headers = _get_headers()
        
        # 軽量なエンドポイントで認証確認
        response = requests.get(f"{base_url}/shop.json", headers=headers, timeout=10)
        
        if response.status_code == 200:
            shop_data = response.json()
            shop_name = shop_data.get('shop', {}).get('name', 'Unknown')
            return True, f"Shopify接続OK: {shop_name}"
        else:
            return False, f"Shopify認証エラー: HTTP {response.status_code}"
            
    except Exception as e:
        return False, f"Shopify接続エラー: {str(e)}"

def check_ga4() -> Tuple[bool, str]:
    """
    GA4の認証情報とプロパティアクセスを確認
    """
    try:
        from src.connectors.ga4 import get_ga4_credentials
        
        # 認証情報の取得を試行
        creds = get_ga4_credentials()
        
        if creds and creds.valid:
            property_id = os.getenv("GA4_PROPERTY_ID")
            return True, f"GA4認証OK: プロパティ {property_id}"
        else:
            return False, "GA4認証情報が無効です"
            
    except Exception as e:
        return False, f"GA4認証エラー: {str(e)}"

def check_square() -> Tuple[bool, str]:
    """
    Squareの軽量エンドポイントで認証確認
    """
    try:
        from src.connectors.square import _get_client
        
        client = _get_client()
        
        # 軽量なエンドポイントで認証確認
        result = client.locations.list_locations()
        
        if result.is_success():
            locations = result.body.get('locations', [])
            location_count = len(locations)
            return True, f"Square接続OK: {location_count}ロケーション"
        else:
            return False, f"Square認証エラー: {result.errors}"
            
    except Exception as e:
        return False, f"Square接続エラー: {str(e)}"

def check_google_ads() -> Tuple[bool, str]:
    """
    Google AdsのOAuthトークン検証と軽量クエリで接続確認
    """
    try:
        from src.connectors.google_ads import _client
        
        client = _client()
        customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
        
        # 軽量なクエリで接続確認（レポート系は避ける）
        query = f"SELECT customer.id FROM customer WHERE customer.id = {customer_id}"
        
        response = client.service.google_ads_service.search(
            customer_id=customer_id,
            query=query
        )
        
        # 結果の存在確認
        results = list(response)
        if results:
            return True, f"Google Ads接続OK: 顧客ID {customer_id}"
        else:
            return False, "Google Ads: 顧客データが見つかりません"
            
    except Exception as e:
        return False, f"Google Ads接続エラー: {str(e)}"

def run_healthcheck() -> HealthStatus:
    s_ok, s_msg = check_shopify()
    g_ok, g_msg = check_ga4()
    sq_ok, sq_msg = check_square()
    ads_ok, ads_msg = check_google_ads()
    return HealthStatus(
        shopify_ok=s_ok,
        ga4_ok=g_ok,
        square_ok=sq_ok,
        google_ads_ok=ads_ok,
        detail={"shopify": s_msg, "ga4": g_msg, "square": sq_msg, "google_ads": ads_msg},
    )

# =========================
# 売上分析・YoY分析機能
# =========================
def get_revenue_summary(selected_month=None):
    """売上サマリーを取得（全期間対応）"""
    try:
        import duckdb
        from datetime import datetime, timedelta
        
        db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
        con = duckdb.connect(db_path, read_only=True)
        
        # 選択された月または現在月を基準にする
        if selected_month:
            target_month = selected_month
        else:
            target_month = datetime.now().strftime('%Y-%m')
        
        # 対象月の売上（注文合計ベース、重複排除）
        current_month = con.execute("""
            SELECT 
                SUM(order_total) as shopify_revenue
            FROM (
                SELECT DISTINCT order_id, order_total, date
                FROM core_shopify 
                WHERE financial_status = 'paid'
                AND strftime(date, '%Y-%m') = ?
            ) shopify_unique
        """, [target_month]).fetchone()
        
        # Square対象月の売上
        current_square = con.execute("""
            SELECT 
                SUM(amount) as square_revenue
            FROM core_square 
            WHERE status = 'COMPLETED'
            AND strftime(date, '%Y-%m') = ?
        """, [target_month]).fetchone()
        
        # 前月の売上
        target_date = datetime.strptime(target_month + '-01', '%Y-%m-%d')
        prev_month_date = (target_date - timedelta(days=1)).strftime('%Y-%m')
        
        prev_month = con.execute("""
            SELECT 
                SUM(order_total) as shopify_revenue
            FROM (
                SELECT DISTINCT order_id, order_total, date
                FROM core_shopify 
                WHERE financial_status = 'paid'
                AND strftime(date, '%Y-%m') = ?
            ) shopify_unique
        """, [prev_month_date]).fetchone()
        
        # Square前月の売上
        prev_square = con.execute("""
            SELECT 
                SUM(amount) as square_revenue
            FROM core_square 
            WHERE status = 'COMPLETED'
            AND strftime(date, '%Y-%m') = ?
        """, [prev_month_date]).fetchone()
        
        # 前年同月の売上
        prev_year_date = (target_date - timedelta(days=365)).strftime('%Y-%m')
        
        prev_year_month = con.execute("""
            SELECT 
                SUM(order_total) as shopify_revenue
            FROM (
                SELECT DISTINCT order_id, order_total, date
                FROM core_shopify 
                WHERE financial_status = 'paid'
                AND strftime(date, '%Y-%m') = ?
            ) shopify_unique
        """, [prev_year_date]).fetchone()
        
        # Square前年同月の売上
        prev_year_square = con.execute("""
            SELECT 
                SUM(amount) as square_revenue
            FROM core_square 
            WHERE status = 'COMPLETED'
            AND strftime(date, '%Y-%m') = ?
        """, [prev_year_date]).fetchone()
        
        con.close()
        
        # データを整理
        current_total = (current_month[0] or 0) + (current_square[0] or 0)
        prev_total = (prev_month[0] or 0) + (prev_square[0] or 0)
        prev_year_total = (prev_year_month[0] or 0) + (prev_year_square[0] or 0)
        
        # MoM成長率
        mom_growth = ((current_total - prev_total) / prev_total * 100) if prev_total > 0 else 0
        
        # YoY成長率
        yoy_growth = ((current_total - prev_year_total) / prev_year_total * 100) if prev_year_total > 0 else 0
        
        return {
            'current_month': current_total,
            'prev_month': prev_total,
            'prev_year_month': prev_year_total,
            'mom_growth': mom_growth,
            'yoy_growth': yoy_growth,
            'target_month': target_month,
            'error': None
        }
        
    except Exception as e:
        return {
            'current_month': 0,
            'prev_month': 0,
            'prev_year_month': 0,
            'mom_growth': 0,
            'yoy_growth': 0,
            'target_month': selected_month or datetime.now().strftime('%Y-%m'),
            'error': str(e)
        }

def get_monthly_trends_with_yoy(selected_month=None):
    """月別トレンドデータとYoY分析を取得（全期間対応）"""
    try:
        import duckdb
        db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
        con = duckdb.connect(db_path, read_only=True)
        
        # 全期間のデータを取得（制限なし）
        shopify_monthly = con.execute("""
            SELECT 
                strftime(date, '%Y-%m') as month,
                COUNT(DISTINCT order_id) as shopify_orders,
                SUM(order_total) as shopify_revenue
            FROM (
                SELECT DISTINCT order_id, order_total, date
                FROM core_shopify 
                WHERE financial_status = 'paid'
            ) shopify_unique
            GROUP BY strftime(date, '%Y-%m')
            ORDER BY month DESC
        """).fetchall()
        
        # 全期間のSquareデータを取得（制限なし）
        square_monthly = con.execute("""
            SELECT 
                strftime(date, '%Y-%m') as month,
                COUNT(*) as square_payments,
                SUM(amount) as square_revenue
            FROM core_square 
            WHERE status = 'COMPLETED'
            GROUP BY strftime(date, '%Y-%m')
            ORDER BY month DESC
        """).fetchall()
        
        con.close()
        
        # データを統合
        shopify_dict = {row[0]: (row[1], row[2]) for row in shopify_monthly}
        square_dict = {row[0]: (row[1], row[2]) for row in square_monthly}
        
        # 全日付を取得
        all_months = sorted(set(shopify_dict.keys()) | set(square_dict.keys()), reverse=True)
        
        # 選択された月でフィルタリング
        if selected_month:
            all_months = [m for m in all_months if m == selected_month]
        
        result = []
        for month in all_months:
            shopify_orders, shopify_revenue = shopify_dict.get(month, (0, 0))
            square_payments, square_revenue = square_dict.get(month, (0, 0))
            total_revenue = shopify_revenue + square_revenue
            
            # 前年同月の売上を計算
            prev_year_month = None
            if len(month) == 7:  # YYYY-MM形式
                year = int(month[:4])
                month_part = month[5:]
                prev_year_month = f'{year-1}-{month_part}'
            
            prev_year_revenue = 0
            if prev_year_month:
                # 前年同月のデータを個別に取得
                prev_shopify_orders, prev_shopify_revenue = shopify_dict.get(prev_year_month, (0, 0))
                prev_square_payments, prev_square_revenue = square_dict.get(prev_year_month, (0, 0))
                prev_year_revenue = prev_shopify_revenue + prev_square_revenue
            
            # YoY成長率を計算
            yoy_growth = 0
            if prev_year_revenue > 0:
                yoy_growth = ((total_revenue - prev_year_revenue) / prev_year_revenue) * 100
            
            result.append((
                month,
                shopify_orders,
                shopify_revenue,
                square_payments,
                square_revenue,
                total_revenue,
                prev_year_revenue,
                yoy_growth
            ))
        
        return result
        
    except Exception as e:
        return []

def get_daily_revenue():
    """日別売上データを取得"""
    try:
        import duckdb
        db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
        con = duckdb.connect(db_path, read_only=True)
        
        # Shopify日別売上（直近30日、重複排除）
        shopify_daily = con.execute("""
            SELECT 
                date,
                COUNT(DISTINCT order_id) as shopify_orders,
                SUM(order_total) as shopify_revenue
            FROM (
                SELECT DISTINCT order_id, order_total, date
                FROM core_shopify 
                WHERE financial_status = 'paid'
                AND date >= CURRENT_DATE - INTERVAL 30 DAY
            ) shopify_unique
            GROUP BY date
            ORDER BY date DESC
        """).fetchall()
        
        # Square日別売上（直近30日）
        square_daily = con.execute("""
            SELECT 
                date,
                COUNT(*) as square_payments,
                SUM(amount) as square_revenue
            FROM core_square 
            WHERE status = 'COMPLETED'
            AND date >= CURRENT_DATE - INTERVAL 30 DAY
            GROUP BY date
            ORDER BY date DESC
        """).fetchall()
        
        con.close()
        
        # データを統合
        shopify_dict = {row[0]: (row[1], row[2]) for row in shopify_daily}
        square_dict = {row[0]: (row[1], row[2]) for row in square_daily}
        
        # 全日付を取得
        all_dates = sorted(set(shopify_dict.keys()) | set(square_dict.keys()), reverse=True)
        
        result = []
        for date in all_dates:
            shopify_orders, shopify_revenue = shopify_dict.get(date, (0, 0))
            square_payments, square_revenue = square_dict.get(date, (0, 0))
            
            result.append((
                date,
                shopify_orders,
                shopify_revenue,
                square_payments,
                square_revenue
            ))
        
        return result
        
    except Exception as e:
        return []
LAST_UPDATE_PATH = Path("data/.last_update.json")
LAST_UPDATE_PATH.parent.mkdir(parents=True, exist_ok=True)

def _write_last_update() -> None:
    LAST_UPDATE_PATH.write_text(json.dumps({"ts": int(time.time())}, ensure_ascii=False))

def read_last_update() -> Optional[int]:
    if LAST_UPDATE_PATH.exists():
        try:
            return json.loads(LAST_UPDATE_PATH.read_text()).get("ts")
        except Exception:
            return None
    return None

def refresh_all_data(include_google_ads: bool = True):
    """
    フル更新（本番APIのみ）。既存のETLパイプラインを呼び出し
    """
    try:
        # 既存のETLパイプラインを実行
        result = subprocess.run([
            sys.executable, "scripts/run_etl.py"
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            st.success("✅ ETLパイプライン実行完了")
            if result.stdout:
                with st.expander("ETL実行ログ"):
                    st.code(result.stdout)
        else:
            st.error("❌ ETLパイプライン実行エラー")
            with st.expander("エラーログ"):
                st.code(result.stderr)
            return
        
        # データ変換を実行
        transform_result = subprocess.run([
            sys.executable, "run_transform.py", "--all"
        ], capture_output=True, text=True, timeout=180)
        
        if transform_result.returncode == 0:
            st.success("✅ データ変換完了")
        else:
            st.warning("⚠️ データ変換でエラーが発生しました")
            with st.expander("変換エラーログ"):
                st.code(transform_result.stderr)
        
        # 最終更新時刻を記録
        _write_last_update()
        
    except subprocess.TimeoutExpired:
        st.error("❌ 処理がタイムアウトしました。ネットワーク状況を確認してください。")
    except Exception as e:
        st.error(f"❌ 予期しないエラー: {str(e)}")

# =========================
# 最終更新ラベル
# =========================
def _human_time(ts: Optional[int]) -> str:
    if not ts:
        return "未実行"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))

def render_status_bar():
    st.caption(f"🕒 最終更新: {_human_time(read_last_update())}")

# =========================
# Streamlit UI 本体
# =========================
st.set_page_config(page_title="Shopify × GA4 ダッシュボード（本番デバッグ）", layout="wide")
st.title("Shopify × GA4 ダッシュボード（本番デバッグ）")

# 1) モックの痕跡を技術的に拒否
try:
    forbid_mock_usage()
except Exception as e:
    st.error("❌ モック禁止: モック関連の設定が検出されました。削除してから再起動してください。")
    st.code(str(e))
    st.stop()

# 2) 必須環境変数チェック
env_status = validate_required_envs()
if env_status.missing:
    st.error("⚠️ 必要な環境変数が不足しています。`.env` に以下を追記して再起動してください。")
    for k in env_status.missing:
        st.code(f"{k}=...")
    st.stop()

# 3) ステータスバー
render_status_bar()

# 4) サイドバー（操作）
with st.sidebar:
    st.header("操作")
    st.write("ヘルスチェックは本番APIへの軽量アクセスで認証/到達性のみ確認します。")

    @safe_action("ヘルスチェック実行中...")
    def _do_health():
        status = run_healthcheck()
        st.success("ヘルスチェック完了")
        # OK/NG を一覧表示
        st.write({
            "Shopify": status.shopify_ok,
            "GA4": status.ga4_ok,
            "Square": status.square_ok,
            "Google Ads": status.google_ads_ok,
        })
        with st.expander("詳細ログ"):
            st.json(status.detail)

    if st.button("🩺 ヘルスチェック", use_container_width=True):
        _do_health()

    st.divider()

    include_ads = st.toggle("Google広告も更新に含める", value=True, help="承認/権限が整っているときのみONにしてください。")

    @safe_action("データ更新中...")
    def _do_refresh():
        refresh_all_data(include_google_ads=include_ads)
        st.success("更新完了")
        # 上部の最終更新を即時反映
        st.rerun()

    if st.button("🔄 データ更新（本番）", use_container_width=True):
        _do_refresh()

    st.divider()
    
    # 既存のStreamlitアプリへのリンク
    st.markdown("### メインダッシュボード")
    if st.button("📊 統合ダッシュボードを開く", use_container_width=True):
        st.info("新しいタブで `streamlit run streamlit_app.py` を実行してください")

st.divider()
st.info("本番データのみで動作中。エラーはUIに日本語で表示されます。必要に応じて左の『🩺 ヘルスチェック』から接続状況を確認してください。")

# 5) メインコンテンツ
st.header("📊 システム状況")

# データベース状態の確認
try:
    import duckdb
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
    
    if os.path.exists(db_path):
        con = duckdb.connect(db_path, read_only=True)
        
        # テーブル一覧
        tables = con.execute("SHOW TABLES").fetchall()
        st.success(f"✅ データベース接続OK: {len(tables)}テーブル")
        
        # 主要テーブルの行数
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            try:
                shopify_count = con.execute("SELECT COUNT(*) FROM core_shopify").fetchone()[0]
                st.metric("Shopify注文", f"{shopify_count:,}件")
            except:
                st.metric("Shopify注文", "N/A")
        
        with col2:
            try:
                square_count = con.execute("SELECT COUNT(*) FROM core_square").fetchone()[0]
                st.metric("Square支払い", f"{square_count:,}件")
            except:
                st.metric("Square支払い", "N/A")
        
        with col3:
            try:
                ga4_count = con.execute("SELECT COUNT(*) FROM core_ga4").fetchone()[0]
                st.metric("GA4セッション", f"{ga4_count:,}件")
            except:
                st.metric("GA4セッション", "N/A")
        
        with col4:
            try:
                ads_count = con.execute("SELECT COUNT(*) FROM core_ads_campaign").fetchone()[0]
                st.metric("Google Ads", f"{ads_count:,}件")
            except:
                st.metric("Google Ads", "N/A")
        
        con.close()
    else:
        st.warning("⚠️ データベースファイルが見つかりません。データ更新を実行してください。")
        
except Exception as e:
    st.error(f"❌ データベース接続エラー: {str(e)}")

st.divider()

# 6) 売上分析セクション
st.header("💰 売上分析")

# 月選択ドロップダウン
st.subheader("📅 分析期間選択")

# 利用可能な月のリストを取得
try:
    import duckdb
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
    con = duckdb.connect(db_path, read_only=True)
    
    # 利用可能な月を取得
    available_months = con.execute("""
        SELECT DISTINCT strftime(date, '%Y-%m') as month
        FROM (
            SELECT date FROM core_shopify WHERE financial_status = 'paid'
            UNION ALL
            SELECT date FROM core_square WHERE status = 'COMPLETED'
        )
        ORDER BY month DESC
    """).fetchall()
    
    month_options = [row[0] for row in available_months]
    con.close()
    
    # デフォルトは現在月
    from datetime import datetime
    current_month = datetime.now().strftime('%Y-%m')
    default_index = month_options.index(current_month) if current_month in month_options else 0
    
    selected_month = st.selectbox(
        "分析対象月を選択してください:",
        options=month_options,
        index=default_index,
        help="選択した月の売上データとYoY分析を表示します"
    )
    
    st.info(f"選択中: **{selected_month}** のデータを分析中...")
    
except Exception as e:
    st.error(f"月選択データ取得エラー: {str(e)}")
    selected_month = None

# 売上サマリーを取得
revenue_data = get_revenue_summary(selected_month)

if 'error' not in revenue_data:
    # 売上メトリクス
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            f"{selected_month}売上", 
            f"¥{revenue_data['current_month']:,.0f}",
            delta=f"{revenue_data['mom_growth']:+.1f}% (MoM)"
        )
    
    with col2:
        st.metric(
            "前月売上", 
            f"¥{revenue_data['prev_month']:,.0f}"
        )
    
    with col3:
        yoy_symbol = "📈" if revenue_data['yoy_growth'] > 0 else "📉" if revenue_data['yoy_growth'] < 0 else "➡️"
        st.metric(
            "前年同月売上", 
            f"¥{revenue_data['prev_year_month']:,.0f}",
            delta=f"{yoy_symbol} {revenue_data['yoy_growth']:+.1f}% (YoY)"
        )
else:
    st.error(f"売上データ取得エラー: {revenue_data['error']}")

# 月別トレンド
st.subheader("📈 月別売上トレンド")
monthly_data = get_monthly_trends_with_yoy(selected_month)

if monthly_data:
    # データフレームとして表示（改善版）
    import pandas as pd
    
    df = pd.DataFrame(monthly_data, columns=['月', 'Shopify注文', 'Shopify売上', 'Square支払い', 'Square売上', '合計売上', '前年同月売上', 'YoY成長率'])
    
    # 数値のフォーマット（表示用）
    df_display = df.copy()
    df_display['Shopify売上'] = df_display['Shopify売上'].apply(lambda x: f"¥{x:,.0f}")
    df_display['Square売上'] = df_display['Square売上'].apply(lambda x: f"¥{x:,.0f}")
    df_display['合計売上'] = df_display['合計売上'].apply(lambda x: f"¥{x:,.0f}")
    df_display['前年同月売上'] = df_display['前年同月売上'].apply(lambda x: f"¥{x:,.0f}")
    df_display['YoY成長率'] = df_display['YoY成長率'].apply(lambda x: f"{x:+.1f}%")
    
    # 成長率に応じて色分け
    def color_yoy(val):
        if isinstance(val, str) and '%' in val:
            try:
                num_val = float(val.replace('%', '').replace('+', ''))
                if num_val > 0:
                    return 'background-color: #d4edda; color: #155724'  # 緑
                elif num_val < 0:
                    return 'background-color: #f8d7da; color: #721c24'  # 赤
                else:
                    return 'background-color: #fff3cd; color: #856404'  # 黄
            except:
                return ''
        return ''
    
    # スタイルを適用（先にhead()を適用してからstyleを適用）
    styled_df = df_display.head(12).style.map(color_yoy, subset=['YoY成長率'])
    
    # 直近12ヶ月のみ表示
    st.subheader("📊 月別データ詳細（直近12ヶ月）")
    st.dataframe(styled_df, use_container_width=True)
    
    # サマリー情報を表示
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_yoy = df['YoY成長率'].mean()
        st.metric("平均YoY成長率", f"{avg_yoy:+.1f}%")
    
    with col2:
        total_orders = df['Shopify注文'].sum() + df['Square支払い'].sum()
        st.metric("総注文数", f"{total_orders:,}件")
    
    with col3:
        total_revenue = df['合計売上'].sum()
        st.metric("総売上", f"¥{total_revenue:,.0f}")
    
    with col4:
        best_month = df.loc[df['YoY成長率'].idxmax(), '月']
        best_growth = df['YoY成長率'].max()
        st.metric("最高成長月", f"{best_month} ({best_growth:+.1f}%)")
    
    if len(df) > 12:
        with st.expander("全25ヶ月分のデータを表示"):
            st.dataframe(df_display.style.map(color_yoy, subset=['YoY成長率']), use_container_width=True)
else:
    st.info("月別データがありません")

# 日別売上（直近30日）
st.subheader("📅 日別売上（直近30日）")
daily_data = get_daily_revenue()

if daily_data:
    import pandas as pd
    
    df_daily = pd.DataFrame(daily_data, columns=['日付', 'Shopify注文', 'Shopify売上', 'Square支払い', 'Square売上'])
    df_daily['Shopify売上'] = df_daily['Shopify売上'].apply(lambda x: f"¥{x:,.0f}")
    df_daily['Square売上'] = df_daily['Square売上'].apply(lambda x: f"¥{x:,.0f}")
    
    # 直近10日のみ表示
    st.dataframe(df_daily.head(10), use_container_width=True)
    
    if len(df_daily) > 10:
        with st.expander("全30日分のデータを表示"):
            st.dataframe(df_daily, use_container_width=True)
else:
    st.info("日別データがありません")

# 7) チャート・グラフ表示
st.header("📊 売上チャート")

if monthly_data:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    
    # 月別売上チャート（YoY比較付き）
    df_chart = pd.DataFrame(monthly_data, columns=['月', 'Shopify注文', 'Shopify売上', 'Square支払い', 'Square売上', '合計売上', '前年同月売上', 'YoY成長率'])
    df_chart['月'] = pd.to_datetime(df_chart['月'] + '-01')
    
    # 選択された月が1つの場合の特別表示
    if selected_month and len(monthly_data) == 1:
        st.info(f"**{selected_month}** の詳細分析")
        
        # 単月の詳細チャート
        month_data = monthly_data[0]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # プラットフォーム別売上構成
            fig_pie = px.pie(
                values=[month_data[2], month_data[4]],  # Shopify売上, Square売上
                names=['Shopify', 'Square'],
                title=f'{selected_month} プラットフォーム別売上構成',
                color_discrete_sequence=['#1f77b4', '#ff7f0e']
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # YoY比較バー
            fig_comparison = go.Figure()
            fig_comparison.add_trace(go.Bar(
                name='今年',
                x=['売上'],
                y=[month_data[5]],  # 合計売上
                marker_color='#1f77b4'
            ))
            fig_comparison.add_trace(go.Bar(
                name='前年同月',
                x=['売上'],
                y=[month_data[6]],  # 前年同月売上
                marker_color='#ff7f0e'
            ))
            fig_comparison.update_layout(
                title=f'{selected_month} YoY比較',
                yaxis_title="売上金額 (¥)",
                barmode='group'
            )
            st.plotly_chart(fig_comparison, use_container_width=True)
    
    else:
        # 複数月のトレンドチャート
        # 売上トレンドチャート（YoY比較）
        fig_revenue = go.Figure()
        
        # 今年の売上
        fig_revenue.add_trace(go.Scatter(
            x=df_chart['月'],
            y=df_chart['合計売上'],
            mode='lines+markers',
            name='今年売上',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8)
        ))
        
        # 前年の売上
        fig_revenue.add_trace(go.Scatter(
            x=df_chart['月'],
            y=df_chart['前年同月売上'],
            mode='lines+markers',
            name='前年同月売上',
            line=dict(color='#ff7f0e', width=2, dash='dash'),
            marker=dict(size=6)
        ))
        
        fig_revenue.update_layout(
            title='月別売上推移（YoY比較）',
            xaxis_title="月",
            yaxis_title="売上金額 (¥)",
            hovermode='x unified',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    # YoY成長率チャート（改善版）
    fig_yoy = go.Figure()
    
    # プラスとマイナスで色分け
    colors = ['red' if x < 0 else 'green' for x in df_chart['YoY成長率']]
    
    fig_yoy.add_trace(go.Bar(
        x=df_chart['月'],
        y=df_chart['YoY成長率'],
        marker_color=colors,
        text=[f'{x:+.1f}%' for x in df_chart['YoY成長率']],
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>YoY成長率: %{y:+.1f}%<extra></extra>'
    ))
    
    # ゼロラインを追加
    fig_yoy.add_hline(y=0, line_dash="dash", line_color="black", opacity=0.5)
    
    fig_yoy.update_layout(
        title={
            'text': '月別YoY成長率',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        xaxis_title="月",
        yaxis_title="YoY成長率 (%)",
        hovermode='x unified',
        showlegend=False,
        height=400,
        margin=dict(l=50, r=50, t=80, b=50)
    )
    
    # X軸の日付フォーマットを改善
    fig_yoy.update_xaxes(
        tickformat='%Y-%m',
        tickangle=45
    )
    
    st.plotly_chart(fig_yoy, use_container_width=True)
    
    # 注文数チャート（改善版）
    fig_orders = go.Figure()
    
    # Shopify注文数
    fig_orders.add_trace(go.Bar(
        name='Shopify注文',
        x=df_chart['月'],
        y=df_chart['Shopify注文'],
        marker_color='#1f77b4',
        text=df_chart['Shopify注文'],
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>Shopify注文: %{y}件<extra></extra>'
    ))
    
    # Square支払い数
    fig_orders.add_trace(go.Bar(
        name='Square支払い',
        x=df_chart['月'],
        y=df_chart['Square支払い'],
        marker_color='#ff7f0e',
        text=df_chart['Square支払い'],
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>Square支払い: %{y}件<extra></extra>'
    ))
    
    fig_orders.update_layout(
        title={
            'text': '月別注文数推移',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        xaxis_title="月",
        yaxis_title="注文数 (件)",
        hovermode='x unified',
        barmode='group',
        height=400,
        margin=dict(l=50, r=50, t=80, b=50),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # X軸の日付フォーマットを改善
    fig_orders.update_xaxes(
        tickformat='%Y-%m',
        tickangle=45
    )
    
    st.plotly_chart(fig_orders, use_container_width=True)
    
    # 売上金額チャート（追加）
    fig_revenue_amount = go.Figure()
    
    # 今年の売上
    fig_revenue_amount.add_trace(go.Bar(
        name='今年売上',
        x=df_chart['月'],
        y=df_chart['合計売上'],
        marker_color='#2E8B57',
        text=[f'¥{x:,.0f}' for x in df_chart['合計売上']],
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>今年売上: ¥%{y:,.0f}<extra></extra>'
    ))
    
    # 前年同月売上
    fig_revenue_amount.add_trace(go.Bar(
        name='前年同月売上',
        x=df_chart['月'],
        y=df_chart['前年同月売上'],
        marker_color='#FF6347',
        text=[f'¥{x:,.0f}' for x in df_chart['前年同月売上']],
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>前年同月売上: ¥%{y:,.0f}<extra></extra>'
    ))
    
    fig_revenue_amount.update_layout(
        title={
            'text': '月別売上金額比較',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        xaxis_title="月",
        yaxis_title="売上金額 (¥)",
        hovermode='x unified',
        barmode='group',
        height=400,
        margin=dict(l=50, r=50, t=80, b=50),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # X軸の日付フォーマットを改善
    fig_revenue_amount.update_xaxes(
        tickformat='%Y-%m',
        tickangle=45
    )
    
    st.plotly_chart(fig_revenue_amount, use_container_width=True)

if daily_data:
    import pandas as pd
    import plotly.express as px
    
    # 日別売上チャート（直近30日）
    df_daily_chart = pd.DataFrame(daily_data, columns=['日付', 'Shopify注文', 'Shopify売上', 'Square支払い', 'Square売上'])
    df_daily_chart['日付'] = pd.to_datetime(df_daily_chart['日付'])
    
    # 日別売上推移
    fig_daily = px.line(
        df_daily_chart, 
        x='日付', 
        y=['Shopify売上', 'Square売上'],
        title='日別売上推移（直近30日）',
        labels={'value': '売上金額 (¥)', 'variable': 'プラットフォーム'}
    )
    fig_daily.update_layout(
        xaxis_title="日付",
        yaxis_title="売上金額 (¥)",
        hovermode='x unified'
    )
    st.plotly_chart(fig_daily, use_container_width=True)
    
    # プラットフォーム別売上比較
    platform_revenue = {
        'Shopify': df_daily_chart['Shopify売上'].sum(),
        'Square': df_daily_chart['Square売上'].sum()
    }
    
    fig_pie = px.pie(
        values=list(platform_revenue.values()),
        names=list(platform_revenue.keys()),
        title='プラットフォーム別売上構成比（直近30日）'
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# フッター
st.divider()
st.markdown("""
---
**開発**: Cursor AI Assistant | **バージョン**: 2.1.0 | **最終更新**: 2025-09-03
""")
