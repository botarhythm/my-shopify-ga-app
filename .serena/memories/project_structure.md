# プロジェクト構造

## ディレクトリ構成
```
my-shopify-ga-app/
├── 📁 data/                    # データフォルダ
│   ├── 📁 raw/                 # 元データ（CSV、認証ファイル）
│   ├── 📁 processed/           # 処理済みデータ
│   ├── 📁 reports/             # 分析レポート
│   └── 📁 ads/                 # Google Ads関連データ
│       ├── 📁 raw/             # 生データ
│       ├── 📁 processed/       # 処理済み
│       └── 📁 cache/           # キャッシュ・フィクスチャ
├── 📁 src/                     # ソースコード
│   ├── 📁 extractors/          # データ抽出
│   ├── 📁 analysis/            # データ分析
│   ├── 📁 ads/                 # Google Ads統合
│   ├── 📁 ga4/                 # GA4関連
│   ├── 📁 shopify/             # Shopify関連
│   ├── 📁 ui/                  # UI関連
│   └── 📁 utils/               # ユーティリティ
├── 📁 config/                  # 設定ファイル
├── 📁 docs/                    # ドキュメント
├── 📁 logs/                    # ログファイル
├── 📁 app_tabs/                # Streamlitタブ
└── streamlit_app.py            # メインアプリ
```

## 主要ファイル
- **streamlit_app.py**: メインダッシュボード
- **src/analysis/run_analysis_pipeline.py**: 分析パイプライン
- **config/requirements.txt**: 依存関係
- **config/google_ads.yaml**: Google Ads設定
- **.env**: 環境変数（API認証情報）

## データフロー
1. **抽出**: `src/extractors/` → `data/raw/`
2. **処理**: `src/analysis/` → `data/processed/`
3. **表示**: Streamlitダッシュボード

## 拡張性
- 新しいデータソース: `src/extractors/`に追加
- 新しい分析: `src/analysis/`に追加
- 新しいUI: `app_tabs/`に追加