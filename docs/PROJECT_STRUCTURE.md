# プロジェクト構造

## 📁 ディレクトリ構造

```
my-shopify-ga-app/
├── 📁 config/                    # 設定ファイル
│   ├── requirements.txt          # Python依存関係
│   ├── google_ads.yaml          # Google Ads設定
│   └── period_validation.yaml   # 期間検証設定
├── 📁 data/                      # データフォルダ
│   ├── 📁 raw/                   # 生データ（CSV、認証ファイル）
│   ├── 📁 processed/             # 処理済みデータ
│   ├── 📁 reports/              # 分析レポート
│   ├── 📁 ads/                   # Google Ads関連データ
│   │   ├── 📁 raw/               # 生データ
│   │   ├── 📁 processed/         # 処理済み
│   │   └── 📁 cache/             # キャッシュ・フィクスチャ
│   └── 📁 duckdb/                # DuckDBデータベース
│       └── commerce.duckdb       # メインデータベース
├── 📁 src/                       # ソースコード
│   ├── 📁 connectors/            # 外部API接続
│   │   ├── __init__.py
│   │   ├── ga4.py               # GA4データ取得
│   │   ├── google_ads.py        # Google Adsデータ取得
│   │   ├── shopify.py           # Shopifyデータ取得
│   │   └── square.py            # Squareデータ取得
│   ├── 📁 ingest/               # データ取り込み
│   │   └── run_incremental.py   # 増分データ取り込み
│   ├── 📁 transform/             # データ変換
│   │   ├── build_core.sql       # コアテーブル構築
│   │   ├── build_marts.sql      # マートテーブル構築
│   │   └── build_yoy.sql        # YoYテーブル構築
│   ├── 📁 quality/               # 品質管理
│   │   ├── checks.sql           # 品質チェックSQL
│   │   └── tests.py             # 品質テスト
│   ├── 📁 app_tabs/              # Streamlitタブ
│   │   ├── kpi.py               # KPIダッシュボード
│   │   ├── details.py           # 詳細分析
│   │   ├── ads.py               # 広告分析
│   │   └── quality.py           # 品質チェック
│   ├── 📁 extractors/            # データ抽出（旧）
│   ├── 📁 analysis/              # データ分析（旧）
│   ├── 📁 ads/                   # Google Ads統合（旧）
│   ├── 📁 ga4/                   # GA4関連（旧）
│   ├── 📁 shopify/               # Shopify関連（旧）
│   ├── 📁 ui/                    # UI関連（旧）
│   └── 📁 utils/                 # ユーティリティ
├── 📁 docs/                      # ドキュメント
├── 📁 logs/                      # ログファイル
├── 📁 app_tabs/                  # Streamlitタブ（旧）
├── streamlit_app.py              # メインアプリ
├── run_transform.py              # データ変換実行
├── env.template                  # 環境変数テンプレート
└── README.md                     # プロジェクト概要
```

## 🏗️ アーキテクチャ概要

### データフロー

```
外部API → Connectors → Ingest → Transform → Streamlit
   ↓         ↓         ↓         ↓          ↓
GA4/Ads/  API接続    DuckDB    SQL変換    ダッシュボード
Shopify/  モジュール   staging   core/marts  表示
Square
```

### レイヤー構造

1. **Connectors Layer** (`src/connectors/`)
   - 外部APIとの接続を担当
   - 各プラットフォーム専用のクライアント
   - エラーハンドリング・リトライ機能

2. **Ingest Layer** (`src/ingest/`)
   - データの取り込みを担当
   - 増分・バックフィル機能
   - DuckDBへのUpsert処理

3. **Transform Layer** (`src/transform/`)
   - データの変換を担当
   - SQLベースのETL処理
   - staging → core → marts → YoY

4. **Quality Layer** (`src/quality/`)
   - データ品質管理を担当
   - 自動チェック・テスト機能
   - 異常値・欠損値検出

5. **App Layer** (`src/app_tabs/`)
   - ユーザーインターフェースを担当
   - Streamlitタブの実装
   - データ可視化・分析機能

## 📊 データベース設計

### テーブル命名規則

- **stg_***: Stagingテーブル（API生データ）
- **core_***: Coreテーブル（正規化済み）
- **mart_***: Martテーブル（分析用集計）

### 主要テーブル

#### Staging Tables
- `stg_ga4`: GA4生データ
- `stg_shopify_orders`: Shopify注文データ
- `stg_shopify_products`: Shopify商品データ
- `stg_square_payments`: Square支払いデータ
- `stg_ads_campaign`: Google Adsキャンペーンデータ
- `stg_ads_adgroup`: Google Ads広告グループデータ
- `stg_ads_keyword`: Google Adsキーワードデータ

#### Core Tables
- `core_ga4`: GA4正規化データ
- `core_shopify_orders`: Shopify注文正規化データ
- `core_shopify_products`: Shopify商品正規化データ
- `core_square_payments`: Square支払い正規化データ
- `core_ads_campaign`: Google Adsキャンペーン正規化データ
- `core_ads_adgroup`: Google Ads広告グループ正規化データ
- `core_ads_keyword`: Google Adsキーワード正規化データ

#### Mart Tables
- `mart_daily`: 日別統合データ
- `mart_revenue_daily`: 日別売上データ
- `mart_traffic_daily`: 日別トラフィックデータ
- `mart_ads_daily`: 日別広告データ
- `mart_product_daily`: 商品別売上データ
- `mart_source_daily`: 流入元別効率データ
- `mart_page_daily`: ページ別効率データ
- `mart_campaign_daily`: キャンペーン別効率データ
- `mart_keyword_daily`: キーワード別効率データ

#### YoY Tables
- `mart_daily_yoy`: 日別YoY比較データ
- `mart_product_yoy`: 商品別YoY比較データ
- `mart_source_yoy`: 流入元別YoY比較データ
- `mart_campaign_yoy`: キャンペーン別YoY比較データ

## 🔧 開発ガイド

### 新しいコネクタの追加

1. `src/connectors/`に新しいファイルを作成
2. 標準的なインターフェースを実装
3. `src/connectors/__init__.py`にエクスポート
4. `src/ingest/run_incremental.py`に統合

### 新しい分析機能の追加

1. `src/transform/`にSQLファイルを作成
2. `src/app_tabs/`にUIコンポーネントを作成
3. `streamlit_app.py`にタブを追加

### 品質チェックの追加

1. `src/quality/checks.sql`にSQLクエリを追加
2. `src/quality/tests.py`にPythonテストを追加
3. `src/app_tabs/quality.py`にUIを追加

## 📈 パフォーマンス最適化

### データ取得最適化

- **バッチ処理**: 30日ずつ分割して取得
- **リトライ機能**: tenacityによる指数バックオフ
- **キャッシュ**: DuckDBのインデックス活用

### クエリ最適化

- **インデックス**: 日付列にインデックス作成
- **パーティション**: 大規模データの分割
- **マテリアライズドビュー**: 集計結果の事前計算

### UI最適化

- **キャッシュ**: Streamlitのキャッシュ機能活用
- **遅延読み込み**: 必要時のみデータ取得
- **ページネーション**: 大量データの分割表示

## 🔒 セキュリティ

### 認証情報管理

- **環境変数**: `.env`ファイルでの管理
- **Git除外**: 認証ファイルの除外設定
- **暗号化**: 機密データの暗号化

### データ保護

- **個人情報**: コア層での匿名化
- **アクセス制御**: 必要最小限の権限
- **監査ログ**: データアクセスの記録

## 📝 ログ・監視

### ログ構造

```json
{
  "operation": "データ取得",
  "context": "GA4",
  "message": "成功",
  "ai_todo": "次のアクション",
  "human_note": "開発者メモ"
}
```

### 監視項目

- **データ品質**: 欠損値・異常値の検出
- **パフォーマンス**: 処理時間・リソース使用量
- **エラー率**: APIエラー・処理エラーの監視
- **データ鮮度**: 最新データの確認

---

**最終更新**: 2025-09-02 | **バージョン**: 2.0.0
