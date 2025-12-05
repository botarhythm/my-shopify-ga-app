# scripts/run_etl.py
import os, sys, datetime as dt
import duckdb, pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

# 環境変数の確認
print(f"環境変数確認:")
print(f"  DUCKDB_PATH: {os.getenv('DUCKDB_PATH', '未設定')}")
print(f"  GA4_PROPERTY_ID: {os.getenv('GA4_PROPERTY_ID', '未設定')}")
print(f"  GOOGLE_ADS_CUSTOMER_ID: {os.getenv('GOOGLE_ADS_CUSTOMER_ID', '未設定')}")

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ===== Connectors（あなたの src/connectors 実装へ合わせて import を調整）=====
from src.connectors.ga4 import fetch_ga4_daily_all
from src.connectors.google_ads import fetch_ads_campaign_daily
from src.connectors.shopify import fetch_orders_incremental
from src.connectors.square import fetch_payments

DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_test.duckdb")
DEFAULT_BACKFILL_DAYS = int(os.getenv("DEFAULT_BACKFILL_DAYS", "400"))

def connect_rw():
    # ディレクトリを作成
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    
    # 既存のファイルが破損している場合は削除
    if os.path.exists(DB):
        try:
            # ファイルサイズを確認
            if os.path.getsize(DB) == 0:
                os.remove(DB)
                print(f"空のDuckDBファイルを削除: {DB}")
        except Exception as e:
            print(f"DuckDBファイル確認エラー: {e}")
            try:
                os.remove(DB)
                print(f"破損したDuckDBファイルを削除: {DB}")
            except:
                pass
    
    # 新しいDuckDBファイルを作成
    try:
        con = duckdb.connect(DB, read_only=False)
        con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
        print(f"DuckDB接続成功: {DB}")
        return con
    except Exception as e:
        print(f"DuckDB接続エラー: {e}")
        # ファイルを削除して再試行
        try:
            os.remove(DB)
            con = duckdb.connect(DB, read_only=False)
            con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
            print(f"DuckDB再接続成功: {DB}")
            return con
        except Exception as e2:
            print(f"DuckDB再接続エラー: {e2}")
            raise

def last_date(con, table, col):
    try:
        d = con.execute(f"SELECT max({col}) FROM {table}").fetchone()[0]
    except duckdb.CatalogException:
        d = None
    return d

def upsert_df(con, df: pd.DataFrame, table: str, pk: list[str]):
    if df is None or df.empty:
        return
    # テーブル作成（なければ）
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
    # 一時テーブルにロード → 擬似MERGE（DELETE+INSERT）
    con.register("df", df)
    cond = " AND ".join([f"t.{k}=s.{k}" for k in pk])
    con.execute("BEGIN")
    con.execute(f"CREATE TEMP TABLE tmp_{table} AS SELECT * FROM df")
    con.execute(f"DELETE FROM {table} t USING tmp_{table} s WHERE {cond}")
    con.execute(f"INSERT INTO {table} SELECT * FROM tmp_{table}")
    con.execute("COMMIT")
    con.unregister("df")

def run_incremental(start: str|None=None, end: str|None=None):
    print(f"ETL処理開始: DB={DB}")
    print(f"現在のディレクトリ: {os.getcwd()}")
    
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    con = connect_rw()
    
    # DuckDBスキーマを初期化
    print("DuckDBスキーマ初期化中...")
    try:
        bootstrap_sql_path = "scripts/bootstrap.sql"
        if not os.path.exists(bootstrap_sql_path):
            print(f"bootstrap.sqlが見つかりません: {bootstrap_sql_path}")
            raise FileNotFoundError(f"bootstrap.sql not found: {bootstrap_sql_path}")
        
        with open(bootstrap_sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        
        # SQLを分割して実行
        sql_statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        for i, stmt in enumerate(sql_statements):
            try:
                con.execute(stmt)
                print(f"SQL実行 {i+1}/{len(sql_statements)}: {stmt[:50]}...")
            except Exception as e:
                print(f"SQL実行エラー {i+1}/{len(sql_statements)}: {e}")
                print(f"   SQL: {stmt[:100]}...")
        
        print("DuckDBスキーマ初期化完了")
    except Exception as e:
        print(f"DuckDBスキーマ初期化エラー: {e}")
        raise

    today = dt.date.today()
    end   = end or today.isoformat()

    # 既存データから開始点を決める（無ければ400日前）
    start_guess = (today - dt.timedelta(days=DEFAULT_BACKFILL_DAYS)).isoformat()
    last_ga4_date = last_date(con, "core_ga4", "date")
    start = start or last_ga4_date or start_guess
    
    print(f"データ取得期間: {start} ～ {end}")

    # === FETCH ===
    print("GA4データ取得中...")
    ga4 = fetch_ga4_daily_all(start, end)
    print(f"GA4データ取得完了: {len(ga4)}行")
    
    # Google Ads（エラーハンドリング付き）
    print("Google Adsデータ取得中...")
    try:
        ads = fetch_ads_campaign_daily(start, end)
        print(f"Google Adsデータ取得完了: {len(ads)}行")
    except Exception as e:
        print(f"Google Ads エラー（スキップ）: {e}")
        ads = None
    
    print("Shopifyデータ取得中...")
    shop = fetch_orders_incremental(created_at_min=start)     # Shopifyは created_at_min= start
    print(f"Shopifyデータ取得完了: {len(shop)}行")
    
    print("Squareデータ取得中...")
    sq   = fetch_payments(start, end)
    print(f"Squareデータ取得完了: {len(sq)}行")

    # === UPSERT to STG相当（core_*に直に格納でもOK）===
    print("データベースに保存中...")
    upsert_df(con, ga4,  "core_ga4", ["date","source","channel","page_path"])
    
    if ads is not None:
        upsert_df(con, ads,  "core_ads_campaign", ["date","campaign_id"])
    
    upsert_df(con, shop, "core_shopify", ["order_id","lineitem_id"])
    upsert_df(con, sq,   "core_square", ["payment_id"])
    print("データベース保存完了")

    # === 変換ビューは bootstrap.sql が作ってくれているので不要 ===
    con.close()
    print("ETL処理完了！")

if __name__ == "__main__":
    # 例：python scripts/run_etl.py で当日まで更新
    try:
        run_incremental()
    except Exception as e:
        print(f"ETL処理エラー: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
