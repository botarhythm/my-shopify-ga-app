# Shopify x GA4 x Square x Google Ads 統合ダッシュボード

**実データ取得による本実装版** - 全API統合完了、本格運用開始

## 🎯 概要

このプロジェクトは、Shopify、Google Analytics 4、Square、Google Adsのデータを統合し、実データ取得による包括的なマーケティング分析ダッシュボードを提供します。

### 主要機能

- **📊 KPIダッシュボード**: 総売上、セッション、CVR、ROAS、YoY比較
- **🔍 詳細分析**: 商品別売上、流入元別効率、ページ分析
- **📈 広告分析**: キャンペーン・キーワード効率、改善提案
- **🔍 品質チェック**: データ品質監視、異常値検出
- **🔄 自動同期**: 増分データ取得、リアルタイム更新

## 🏗️ アーキテクチャ

```
src/
├── connectors/          # 外部API接続
│   ├── ga4.py          # GA4データ取得
│   ├── google_ads.py   # Google Adsデータ取得
│   ├── shopify.py      # Shopifyデータ取得
│   └── square.py       # Squareデータ取得
├── ingest/             # データ取り込み
│   └── run_incremental.py
├── transform/          # データ変換
│   ├── build_core.sql  # コアテーブル構築
│   ├── build_marts.sql # マートテーブル構築
│   └── build_yoy.sql   # YoYテーブル構築
├── quality/            # 品質管理
│   ├── checks.sql      # 品質チェックSQL
│   └── tests.py        # 品質テスト
└── app_tabs/           # Streamlitタブ
    ├── kpi.py          # KPIダッシュボード
    ├── details.py      # 詳細分析
    ├── ads.py          # 広告分析
    └── quality.py      # 品質チェック
```

## 🚀 セットアップ

### 1. 依存関係のインストール

```bash
pip install -r config/requirements.txt
```

### 2. 環境変数の設定

`env.template` をコピーして `.env` ファイルを作成し、各APIの認証情報を設定してください：

```bash
cp env.template .env
```

#### 必要な環境変数

```env
# === GA4 ===
GA4_PROPERTY_ID=xxxxxxxx
GOOGLE_APPLICATION_CREDENTIALS=./secrets/ga-sa.json

# === Google Ads ===
GOOGLE_ADS_DEVELOPER_TOKEN=xxxx
GOOGLE_ADS_CLIENT_ID=xxxx.apps.googleusercontent.com
GOOGLE_ADS_CLIENT_SECRET=xxxx
GOOGLE_ADS_REFRESH_TOKEN=xxxx
GOOGLE_ADS_LOGIN_CUSTOMER_ID=1234567890      # MCC (無ければ空)
GOOGLE_ADS_CUSTOMER_ID=1234567890            # 対象アカウント

# === Shopify ===
SHOPIFY_SHOP_URL=your-shop.myshopify.com
SHOPIFY_ACCESS_TOKEN=shpat_xxx

# === Square ===
SQUARE_ACCESS_TOKEN=EAAA-xxx
SQUARE_LOCATION_ID=XXXXXXX

# === DB ===
DUCKDB_PATH=./data/duckdb/commerce.duckdb

# === App ===
DEFAULT_BACKFILL_DAYS=400   # 初回は約13ヶ月
TIMEZONE=Asia/Tokyo
```

### 3. API認証の設定

#### GA4
1. Google Cloud Consoleでサービスアカウントを作成
2. GA4プロパティにサービスアカウントを追加
3. JSONキーファイルを `secrets/ga-sa.json` に保存

#### Google Ads
1. Google Ads APIアクセスを有効化
2. OAuth2.0認証情報を作成
3. リフレッシュトークンを取得

#### Shopify
1. プライベートアプリを作成
2. 必要な権限を付与（Orders, Products, Customers）
3. アクセストークンを取得

#### Square
1. Square Developer Dashboardでアプリを作成
2. アクセストークンを取得
3. ロケーションIDを確認

## 📊 使用方法

### 1. データベース初期化

```bash
# DuckDBデータベースの初期化
python scripts/bootstrap_duckdb.py
```

### 2. データ取得・変換の実行

```bash
# ETLパイプラインの実行（データ取得・変換・統合）
python scripts/run_etl.py
```

### 3. Streamlitアプリの起動

```bash
streamlit run streamlit_app.py
```

### 4. 日次同期の設定

#### Windows Task Scheduler
```batch
# 毎日午前2時に実行
schtasks /create /tn "DataSync" /tr "python scripts/run_etl.py" /sc daily /st 02:00
```

#### Linux/macOS cron
```bash
# crontab -e
0 2 * * * cd /path/to/project && python scripts/run_etl.py
```

## 📈 ダッシュボード機能

### KPIダッシュボード
- **総売上**: Shopify + Square の統合売上
- **セッション数**: GA4からのトラフィック
- **コンバージョン率**: 購入率の推移
- **ROAS**: 広告費用対効果
- **YoY比較**: 前年同期との比較

### 詳細分析
- **商品分析**: 売上上位商品、販売数量
- **流入元分析**: セッション数、CVR、セッション単価
- **ページ分析**: PV、CVR、改善提案

### 広告分析
- **キャンペーン分析**: 費用、ROAS、CTR、CVR
- **キーワード分析**: 除外候補、入札下げ候補
- **改善提案**: 自動生成される具体的アクション

### 品質チェック
- **欠損データ検出**: データの完全性確認
- **異常値検出**: 統計的異常値の特定
- **整合性チェック**: データ間の整合性確認
- **データ鮮度**: 最新データの確認

## 🔧 開発・運用

### データ更新

```bash
# 全データ更新
python scripts/run_etl.py

# データベース状態確認
python scripts/health_check.py
```

### 品質チェック

```bash
# データベース状態確認
python scripts/health_check.py

# 品質テスト実行
pytest src/quality/tests.py
```

### トラブルシューティング

#### よくある問題

1. **データが表示されない**
   - データベース初期化: `python scripts/bootstrap_duckdb.py`
   - ETLパイプライン実行: `python scripts/run_etl.py`

2. **API認証エラー**
   - 環境変数を確認: `.env` ファイルの設定
   - トークンの有効性を確認

3. **データが古い**
   - ETLパイプライン実行: `python scripts/run_etl.py`
   - 品質チェックタブでデータ状態を確認

## 📚 技術仕様

### データベース
- **DuckDB**: 軽量な分析用データベース
- **テーブル構造**: staging → core → marts → YoY

### データフロー
1. **取得**: 各APIからデータを取得
2. **取り込み**: stagingテーブルに保存
3. **変換**: core → marts → YoYテーブルを構築
4. **可視化**: Streamlitでダッシュボード表示

### 品質管理
- **自動チェック**: SQLベースの品質チェック
- **テスト**: pytestによる自動テスト
- **監視**: 異常値・欠損値の自動検出

## 🤝 貢献

1. このリポジトリをフォーク
2. 機能ブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. プルリクエストを作成

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 📞 サポート

問題や質問がある場合は、以下をご確認ください：

1. **ドキュメント**: このREADMEファイル
2. **品質チェック**: Streamlitアプリの品質チェックタブ
3. **ログ**: 各スクリプトの実行ログ

---

**開発**: Cursor AI Assistant | **バージョン**: 2.1.0 | **最終更新**: 2025-09-03
