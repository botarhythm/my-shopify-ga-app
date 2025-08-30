# 📁 プロジェクト構造

## 概要
Shopify x GA4 x Square 統合ダッシュボードの整理されたプロジェクト構造

## 📂 フォルダ構造

```
my-shopify-ga-app/
├── 📁 data/                    # データフォルダ
│   ├── 📁 raw/                 # 元データ（CSV、トークン等）
│   │   ├── shopify_orders_*.csv
│   │   ├── shopify_products_*.csv
│   │   ├── ga4_data_*.csv
│   │   ├── square_payments_*.csv
│   │   ├── token.pickle
│   │   └── client_secret_*.json
│   ├── 📁 processed/           # 処理済みデータ（将来的に使用）
│   └── 📁 reports/             # 分析レポート・チャート
│       ├── analysis_report_*.md
│       ├── analysis_charts_*.png
│       ├── strategy_report_*.md
│       └── cross_analysis_*.md
├── 📁 src/                     # ソースコード
│   ├── 📁 extractors/          # データ抽出スクリプト
│   │   ├── shopify_data_extractor.py
│   │   ├── ga4_data_extractor.py
│   │   └── square_data_extractor.py
│   ├── 📁 analysis/            # 分析スクリプト
│   │   ├── data_analyzer.py
│   │   ├── cross_analysis_30days.py
│   │   ├── strategy_proposer.py
│   │   └── run_analysis_pipeline.py
│   └── 📁 utils/                # ユーティリティ
│       └── test_ga4.py
├── 📁 config/                  # 設定ファイル
│   └── requirements.txt
├── 📁 docs/                    # ドキュメント
│   ├── README.md
│   └── PROJECT_STRUCTURE.md
└── streamlit_app.py            # メインアプリケーション
```

## 🔧 使用方法

### 1. データ抽出
```bash
# Shopifyデータ取得
python src/extractors/shopify_data_extractor.py

# GA4データ取得
python src/extractors/ga4_data_extractor.py

# Squareデータ取得
python src/extractors/square_data_extractor.py
```

### 2. 分析実行
```bash
# 全パイプライン実行
python src/analysis/run_analysis_pipeline.py

# 個別分析
python src/analysis/data_analyzer.py
python src/analysis/strategy_proposer.py
```

### 3. ダッシュボード起動
```bash
streamlit run streamlit_app.py
```

## 📊 データフロー

1. **データ抽出**: `src/extractors/` → `data/raw/`
2. **データ分析**: `data/raw/` → `data/reports/`
3. **ダッシュボード表示**: `data/raw/` + `data/reports/`

## 🎯 利点

- **整理された構造**: データ、コード、設定が明確に分離
- **保守性向上**: 関連ファイルがグループ化されている
- **拡張性**: 新しい機能を適切なフォルダに追加可能
- **データ管理**: 元データと処理済みデータが分離

## 📝 注意事項

- CSVファイルは `data/raw/` フォルダに配置してください
- 新しいスクリプトは適切な `src/` サブフォルダに配置してください
- 設定ファイルは `config/` フォルダに配置してください
