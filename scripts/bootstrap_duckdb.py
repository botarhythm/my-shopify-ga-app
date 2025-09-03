# scripts/bootstrap_duckdb.py
import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_test.duckdb")

def bootstrap():
    """DuckDBスキーマ初期化"""
    print(f"DuckDB初期化開始: {DB}")
    print(f"現在のディレクトリ: {os.getcwd()}")
    
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
    except Exception as e:
        print(f"DuckDB接続エラー: {e}")
        # ファイルを削除して再試行
        try:
            os.remove(DB)
            con = duckdb.connect(DB, read_only=False)
            con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
            print(f"DuckDB再接続成功: {DB}")
        except Exception as e2:
            print(f"DuckDB再接続エラー: {e2}")
            raise
    
    # ブートストラップSQLを実行
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
        
        con.close()
        print(f"DuckDB初期化完了: {DB}")
    except Exception as e:
        print(f"DuckDB初期化エラー: {e}")
        con.close()
        raise

if __name__ == "__main__":
    try:
        bootstrap()
    except Exception as e:
        print(f"ブートストラップエラー: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
