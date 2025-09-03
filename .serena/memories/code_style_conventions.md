# コードスタイル・規約

## ファイル構成
- **エンコーディング**: UTF-8
- **ファイルヘッダー**: `#!/usr/bin/env python3` + `# -*- coding: utf-8 -*-`
- **docstring**: 関数・クラスの説明を日本語で記述

## 命名規則
- **変数・関数**: snake_case (`total_revenue`, `calculate_yoy_delta`)
- **クラス**: PascalCase (`GoogleAdsClientFactory`, `CampaignRow`)
- **定数**: UPPER_SNAKE_CASE (`GOOGLE_ADS_CUSTOMER_ID`)
- **ファイル名**: snake_case (`google_ads_client.py`, `run_analysis_pipeline.py`)

## 型ヒント
- **関数の戻り値**: `-> str | None`, `-> pd.DataFrame`
- **Union型**: `str | None` (Python 3.10+形式)
- **Pydantic**: データスキーマ定義に使用

## docstring形式
```python
def calculate_yoy_delta(current_value: float, previous_value: float, is_currency: bool = True) -> tuple[str, str]:
    \"\"\"昨年同期対比のデルタを計算\"\"\"
```

## エラーハンドリング
- **try-except**: 具体的な例外処理
- **ログ出力**: エラー内容を日本語で表示
- **デフォルト値**: データ不足時の適切なフォールバック

## データ処理パターン
- **数値変換**: `pd.to_numeric(errors='coerce').fillna(0)`
- **日付処理**: `pd.to_datetime()`
- **安全なアクセス**: 辞書・DataFrameの存在チェック

## UI表示
- **日本語**: ユーザー向けメッセージは日本語
- **通貨表示**: `¥{amount:,.0f}` 形式
- **パーセント**: `{percentage:.1f}%` 形式