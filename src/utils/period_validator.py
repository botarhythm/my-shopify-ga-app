#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期間設定検証モジュール
データ収集・分析時の期間設定を検証し、不一致を検出・修正します。
"""

import os
import yaml
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import streamlit as st

class PeriodValidator:
    """期間設定の検証と修正を行うクラス"""
    
    def __init__(self, config_path: str = "config/period_validation.yaml"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルのパス
        """
        self.config = self._load_config(config_path)
        self.validation_results = []
    
    def _load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # デフォルト設定
            return {
                'period_validation': {
                    'expected_periods': {
                        'august_2025': {
                            'start_date': '2025-08-01',
                            'end_date': '2025-08-31',
                            'description': '2025年8月の完全な期間',
                            'required_days': 31
                        }
                    },
                    'validation_rules': {
                        'check_completeness': True,
                        'auto_complete_missing_dates': True,
                        'warning_threshold': 0.9
                    }
                }
            }
    
    def validate_data_period(self, df: pd.DataFrame, expected_period: str, 
                           data_source: str = "ga4") -> Dict:
        """
        データの期間を検証
        
        Args:
            df: 検証するデータフレーム
            expected_period: 期待される期間のキー
            data_source: データソース名
        
        Returns:
            検証結果の辞書
        """
        if df is None or df.empty:
            return {
                'valid': False,
                'error': 'データが空です',
                'suggestions': ['データファイルを確認してください']
            }
        
        # 期待される期間を取得
        expected_config = self.config['period_validation']['expected_periods'].get(expected_period)
        if not expected_config:
            return {
                'valid': False,
                'error': f'期待される期間 "{expected_period}" が設定にありません',
                'suggestions': ['設定ファイルを確認してください']
            }
        
        expected_start = pd.to_datetime(expected_config['start_date'])
        expected_end = pd.to_datetime(expected_config['end_date'])
        required_days = expected_config['required_days']
        
        # 実際のデータ期間を取得
        if 'date' not in df.columns:
            return {
                'valid': False,
                'error': '日付列が見つかりません',
                'suggestions': ['データの列名を確認してください']
            }
        
        # 日付列をdatetime型に変換
        df['date'] = pd.to_datetime(df['date'])
        actual_start = df['date'].min()
        actual_end = df['date'].max()
        
        # 期間の比較
        period_match = (actual_start == expected_start) and (actual_end == expected_end)
        
        # データの完全性をチェック
        date_range = pd.date_range(start=expected_start, end=expected_end, freq='D')
        existing_dates = set(df['date'].dt.date.unique())
        missing_dates = [d.date() for d in date_range if d.date() not in existing_dates]
        
        completeness_ratio = len(existing_dates) / len(date_range)
        
        # 検証結果を構築
        result = {
            'valid': period_match and completeness_ratio >= self.config['period_validation']['validation_rules']['warning_threshold'],
            'period_match': period_match,
            'completeness_ratio': completeness_ratio,
            'expected_start': expected_start,
            'expected_end': expected_end,
            'actual_start': actual_start,
            'actual_end': actual_end,
            'missing_dates': missing_dates,
            'total_days': len(date_range),
            'existing_days': len(existing_dates),
            'missing_days': len(missing_dates),
            'suggestions': []
        }
        
        # 改善提案を生成
        if not period_match:
            result['suggestions'].append(f'期間が一致しません: 期待 {expected_start.date()}〜{expected_end.date()}, 実際 {actual_start.date()}〜{actual_end.date()}')
        
        if missing_dates:
            result['suggestions'].append(f'欠損日付: {len(missing_dates)}日分')
            result['suggestions'].append('データ補完機能を使用してください')
        
        if completeness_ratio < self.config['period_validation']['validation_rules']['warning_threshold']:
            result['suggestions'].append(f'データの完全性が不足: {completeness_ratio:.1%}')
        
        return result
    
    def complete_missing_dates(self, df: pd.DataFrame, expected_period: str, 
                              data_source: str = "ga4") -> pd.DataFrame:
        """
        欠損日付を補完
        
        Args:
            df: 元のデータフレーム
            expected_period: 期待される期間のキー
            data_source: データソース名
        
        Returns:
            補完されたデータフレーム
        """
        if df is None or df.empty:
            return df
        
        expected_config = self.config['period_validation']['expected_periods'].get(expected_period)
        if not expected_config:
            return df
        
        expected_start = pd.to_datetime(expected_config['start_date'])
        expected_end = pd.to_datetime(expected_config['end_date'])
        
        # 欠損日付を特定
        date_range = pd.date_range(start=expected_start, end=expected_end, freq='D')
        existing_dates = set(df['date'].dt.date.unique())
        missing_dates = [d.date() for d in date_range if d.date() not in existing_dates]
        
        if not missing_dates:
            return df
        
        # 欠損データを作成
        missing_data = []
        for missing_date in missing_dates:
            # データソースに応じて適切なデフォルト値を設定
            if data_source == "ga4":
                sources = df['source'].unique() if 'source' in df.columns else ['(direct)']
                for source in sources:
                    missing_row = {
                        'date': missing_date,
                        'source': source,
                        'sessions': '0',
                        'totalRevenue': '0'
                    }
                    missing_data.append(missing_row)
            else:
                # その他のデータソース用のデフォルト処理
                missing_row = {
                    'date': missing_date,
                    'sessions': '0',
                    'totalRevenue': '0'
                }
                missing_data.append(missing_row)
        
        # 欠損データを追加
        missing_df = pd.DataFrame(missing_data)
        completed_df = pd.concat([df, missing_df], ignore_index=True)
        
        return completed_df
    
    def generate_validation_report(self) -> str:
        """検証結果のレポートを生成"""
        if not self.validation_results:
            return "検証結果がありません"
        
        report = "# 📊 期間設定検証レポート\n\n"
        report += f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n"
        
        for result in self.validation_results:
            report += f"## 🔍 {result.get('data_source', 'Unknown')} データ\n\n"
            report += f"- **検証結果**: {'✅ 正常' if result['valid'] else '❌ 問題あり'}\n"
            report += f"- **期間一致**: {'✅' if result['period_match'] else '❌'}\n"
            report += f"- **データ完全性**: {result['completeness_ratio']:.1%}\n"
            report += f"- **期待される期間**: {result['expected_start'].date()} 〜 {result['expected_end'].date()}\n"
            report += f"- **実際の期間**: {result['actual_start'].date()} 〜 {result['actual_end'].date()}\n"
            report += f"- **欠損日数**: {result['missing_days']}日\n\n"
            
            if result['suggestions']:
                report += "### 💡 改善提案\n"
                for suggestion in result['suggestions']:
                    report += f"- {suggestion}\n"
                report += "\n"
        
        return report

def validate_and_fix_period_issues(df: pd.DataFrame, expected_period: str = "august_2025", 
                                 data_source: str = "ga4", auto_fix: bool = True) -> Tuple[pd.DataFrame, Dict]:
    """
    期間の問題を検証し、必要に応じて修正する
    
    Args:
        df: 検証するデータフレーム
        expected_period: 期待される期間のキー
        data_source: データソース名
        auto_fix: 自動修正を行うかどうか
    
    Returns:
        修正されたデータフレームと検証結果
    """
    validator = PeriodValidator()
    
    # 検証実行
    validation_result = validator.validate_data_period(df, expected_period, data_source)
    
    # 自動修正が有効で問題がある場合
    if auto_fix and not validation_result['valid']:
        if validation_result['missing_dates']:
            df = validator.complete_missing_dates(df, expected_period, data_source)
            # 修正後の再検証
            validation_result = validator.validate_data_period(df, expected_period, data_source)
    
    return df, validation_result

# Streamlit用の簡易関数
def st_validate_period(df: pd.DataFrame, expected_period: str = "august_2025", 
                      data_source: str = "ga4") -> bool:
    """
    Streamlitアプリ用の期間検証関数
    
    Args:
        df: 検証するデータフレーム
        expected_period: 期待される期間のキー
        data_source: データソース名
    
    Returns:
        検証が成功したかどうか
    """
    validator = PeriodValidator()
    result = validator.validate_data_period(df, expected_period, data_source)
    
    if not result['valid']:
        st.warning(f"""
        ⚠️ **期間設定の問題を検出しました**
        
        **期待される期間**: {result['expected_start'].date()} 〜 {result['expected_end'].date()}
        **実際の期間**: {result['actual_start'].date()} 〜 {result['actual_end'].date()}
        **データ完全性**: {result['completeness_ratio']:.1%}
        **欠損日数**: {result['missing_days']}日
        
        **改善提案**:
        """)
        
        for suggestion in result['suggestions']:
            st.write(f"- {suggestion}")
        
        # 自動修正ボタン
        if st.button("🔧 自動修正を実行"):
            fixed_df = validator.complete_missing_dates(df, expected_period, data_source)
            st.success("✅ データ補完が完了しました")
            return True
    
    return result['valid']