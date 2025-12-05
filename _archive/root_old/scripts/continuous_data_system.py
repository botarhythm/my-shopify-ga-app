#!/usr/bin/env python3
"""
継続的データ活用システム
定期的なデータ取得と分析の自動化
"""
import os
import sys
import schedule
import time
import datetime as dt
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fetch_extended_data import ExtendedDataFetcher

class ContinuousDataSystem:
    """継続的データ活用システム"""
    
    def __init__(self):
        self.db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
        self.log_file = Path("logs/data_fetch.log")
        
        # ログディレクトリを作成
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
    
    def log(self, message: str):
        """ログを記録"""
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        
        print(log_message)
        
        # ログファイルに記録
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_message + "\n")
    
    def daily_data_fetch(self):
        """日次データ取得"""
        self.log("=== 日次データ取得開始 ===")
        
        try:
            fetcher = ExtendedDataFetcher(self.db_path)
            fetcher.run_extended_fetch()
            self.log("日次データ取得完了")
            
        except Exception as e:
            self.log(f"日次データ取得エラー: {e}")
    
    def weekly_full_refresh(self):
        """週次フルリフレッシュ"""
        self.log("=== 週次フルリフレッシュ開始 ===")
        
        try:
            # データベースをバックアップ
            if os.path.exists(self.db_path):
                backup_path = f"{self.db_path}.backup.{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                import shutil
                shutil.copy2(self.db_path, backup_path)
                self.log(f"データベースをバックアップ: {backup_path}")
            
            # 拡張データ取得を実行
            fetcher = ExtendedDataFetcher(self.db_path)
            fetcher.run_extended_fetch()
            
            self.log("週次フルリフレッシュ完了")
            
        except Exception as e:
            self.log(f"週次フルリフレッシュエラー: {e}")
    
    def monthly_analysis(self):
        """月次分析レポート生成"""
        self.log("=== 月次分析レポート生成開始 ===")
        
        try:
            # 月次分析レポートを生成
            from src.analysis.monthly_report import generate_monthly_report
            report_path = generate_monthly_report()
            self.log(f"月次分析レポート生成完了: {report_path}")
            
        except Exception as e:
            self.log(f"月次分析レポート生成エラー: {e}")
    
    def setup_schedule(self):
        """スケジュールを設定"""
        self.log("スケジュール設定中...")
        
        # 毎日午前2時に日次データ取得
        schedule.every().day.at("02:00").do(self.daily_data_fetch)
        
        # 毎週日曜日午前3時に週次フルリフレッシュ
        schedule.every().sunday.at("03:00").do(self.weekly_full_refresh)
        
        # 毎月1日午前4時に月次分析
        schedule.every().month.do(self.monthly_analysis)
        
        self.log("スケジュール設定完了")
        self.log("  - 日次データ取得: 毎日 02:00")
        self.log("  - 週次フルリフレッシュ: 毎週日曜日 03:00")
        self.log("  - 月次分析: 毎月1日 04:00")
    
    def run(self):
        """システムを実行"""
        self.log("=== 継続的データ活用システム開始 ===")
        
        # スケジュールを設定
        self.setup_schedule()
        
        # 初回実行（即座に）
        self.log("初回データ取得を実行中...")
        self.daily_data_fetch()
        
        # スケジュール実行ループ
        self.log("スケジュール実行ループ開始")
        while True:
            schedule.run_pending()
            time.sleep(60)  # 1分ごとにチェック

def main():
    """メイン関数"""
    print("継続的データ活用システムを開始します...")
    print("Ctrl+C で停止できます")
    
    system = ContinuousDataSystem()
    
    try:
        system.run()
    except KeyboardInterrupt:
        system.log("システムを停止しました")
        print("\nシステムを停止しました")
    except Exception as e:
        system.log(f"システムエラー: {e}")
        print(f"システムエラー: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
