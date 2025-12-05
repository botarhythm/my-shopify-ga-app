#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
データ変換実行スクリプト
DuckDBのSQL変換を実行
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def run_sql_file(db_path: str, sql_file: str):
    """SQLファイルを実行"""
    if not os.path.exists(sql_file):
        print(f"❌ SQLファイルが見つかりません: {sql_file}")
        return False
    
    try:
        cmd = f'duckdb "{db_path}" -c ".read {sql_file}"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ {sql_file} を実行しました")
            return True
        else:
            print(f"❌ {sql_file} の実行に失敗しました")
            print(f"エラー: {result.stderr}")
            return False
    
    except Exception as e:
        print(f"❌ {sql_file} の実行中にエラーが発生しました: {e}")
        return False


def run_transform_pipeline():
    """変換パイプラインを実行"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    
    # データベースディレクトリを作成
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    print("🔄 データ変換パイプラインを開始します...")
    
    # SQLファイルのパス
    sql_files = [
        "src/transform/build_core.sql",
        "src/transform/build_marts.sql", 
        "src/transform/build_yoy.sql"
    ]
    
    success_count = 0
    
    for sql_file in sql_files:
        print(f"📝 {sql_file} を実行中...")
        if run_sql_file(db_path, sql_file):
            success_count += 1
    
    print(f"\n✅ 変換完了: {success_count}/{len(sql_files)} ファイルが成功")
    
    if success_count == len(sql_files):
        print("🎉 全ての変換が正常に完了しました")
        return True
    else:
        print("⚠️ 一部の変換でエラーが発生しました")
        return False


def run_quality_checks():
    """品質チェックを実行"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    
    print("🔍 品質チェックを実行中...")
    
    try:
        cmd = f'duckdb "{db_path}" -c ".read src/quality/checks.sql"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ 品質チェックが完了しました")
            if result.stdout.strip():
                print("📋 品質チェック結果:")
                print(result.stdout)
            else:
                print("✅ 品質問題は検出されませんでした")
            return True
        else:
            print(f"❌ 品質チェックでエラーが発生しました: {result.stderr}")
            return False
    
    except Exception as e:
        print(f"❌ 品質チェックの実行中にエラーが発生しました: {e}")
        return False


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description="データ変換実行")
    parser.add_argument("--transform", action="store_true", help="変換パイプラインを実行")
    parser.add_argument("--quality", action="store_true", help="品質チェックを実行")
    parser.add_argument("--all", action="store_true", help="変換と品質チェックを両方実行")
    
    args = parser.parse_args()
    
    if args.all or args.transform:
        success = run_transform_pipeline()
        if not success:
            sys.exit(1)
    
    if args.all or args.quality:
        success = run_quality_checks()
        if not success:
            sys.exit(1)
    
    if not any([args.transform, args.quality, args.all]):
        print("使用方法:")
        print("  python run_transform.py --transform  # 変換のみ")
        print("  python run_transform.py --quality    # 品質チェックのみ")
        print("  python run_transform.py --all        # 両方実行")


if __name__ == "__main__":
    main()
