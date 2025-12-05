# 本実装環境変数設定ガイド

## ■ 概要
本実装では実際のAPI接続を使用してデータを取得します。以下の手順で環境変数を設定してください。

## ■ 必要なAPI認証情報

### 1. Google Analytics 4 (GA4)
- **GA4_PROPERTY_ID**: GA4プロパティID（例: 123456789）
- **GOOGLE_APPLICATION_CREDENTIALS**: サービスアカウントキーファイルのパス

**取得手順**:
1. Google Analytics 4 プロパティにアクセス
2. 管理 → プロパティ設定 → プロパティIDをコピー
3. Google Cloud Consoleでサービスアカウントキーを作成
4. GA4プロパティにサービスアカウントを追加（閲覧者権限）

### 2. Google Ads
- **GOOGLE_ADS_DEVELOPER_TOKEN**: Google Ads API デベロッパートークン
- **GOOGLE_ADS_CLIENT_ID**: OAuth 2.0 クライアントID
- **GOOGLE_ADS_CLIENT_SECRET**: OAuth 2.0 クライアントシークレット
- **GOOGLE_ADS_REFRESH_TOKEN**: リフレッシュトークン
- **GOOGLE_ADS_CUSTOMER_ID**: 対象アカウントID

**取得手順**:
1. Google Ads API デベロッパートークンを申請
2. Google Cloud ConsoleでOAuth 2.0認証情報を作成
3. 認証フローを実行してリフレッシュトークンを取得

### 3. Shopify
- **SHOPIFY_SHOP_URL**: ショップURL（例: yourshop.myshopify.com）
- **SHOPIFY_ACCESS_TOKEN**: プライベートアプリアクセストークン

**取得手順**:
1. Shopify管理画面にアクセス
2. アプリ → プライベートアプリを作成
3. 必要な権限を設定（orders, products, customers）
4. アクセストークンをコピー

### 4. Square
- **SQUARE_ACCESS_TOKEN**: Square API アクセストークン
- **SQUARE_LOCATION_ID**: ロケーションID

**取得手順**:
1. Square Developer Dashboardにアクセス
2. アプリケーションを作成
3. アクセストークンを生成
4. ロケーションIDを取得

## ■ 環境変数設定手順

### 1. .envファイルの作成
```bash
# プロジェクトルートで.envファイルを作成
cp env.template .env
```

### 2. .envファイルの編集
```bash
# 実際の値を設定
GA4_PROPERTY_ID=123456789
GOOGLE_APPLICATION_CREDENTIALS=./data/raw/ga-sa.json

GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_CLIENT_ID=your_client_id.apps.googleusercontent.com
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
GOOGLE_ADS_CUSTOMER_ID=1234567890

SHOPIFY_SHOP_URL=yourshop.myshopify.com
SHOPIFY_ACCESS_TOKEN=shpat_your_access_token

SQUARE_ACCESS_TOKEN=EAAA-your_access_token
SQUARE_LOCATION_ID=your_location_id

DUCKDB_PATH=./data/duckdb/commerce.duckdb
DEFAULT_BACKFILL_DAYS=400
TIMEZONE=Asia/Tokyo
```

### 3. 認証ファイルの配置
```bash
# Google Analytics サービスアカウントキー
# ./data/raw/ga-sa.json に配置

# Google Ads OAuth認証情報
# ./data/raw/client_secret_*.json に配置
```

## ■ 接続テスト

### 1. 個別接続テスト
```bash
# GA4接続テスト
python test_ga4_connection.py

# Shopify接続テスト
python test_shopify_square.py
```

### 2. 統合テスト
```bash
# 全データソースの接続テスト
python scripts/run_etl.py
```

## ■ トラブルシューティング

### よくあるエラーと対処法

#### GA4関連
- **"GA4_PROPERTY_ID 環境変数が設定されていません"**
  - .envファイルにGA4_PROPERTY_IDを設定
  - プロパティIDが正しいか確認

- **"Permission denied"**
  - サービスアカウントにGA4プロパティの閲覧権限を付与
  - サービスアカウントキーファイルのパスが正しいか確認

#### Google Ads関連
- **"Developer token not found"**
  - GOOGLE_ADS_DEVELOPER_TOKENが正しく設定されているか確認
  - デベロッパートークンが有効か確認

- **"Invalid refresh token"**
  - OAuth認証フローを再実行
  - リフレッシュトークンを再取得

#### Shopify関連
- **"Invalid API key"**
  - SHOPIFY_ACCESS_TOKENが正しく設定されているか確認
  - プライベートアプリの権限設定を確認

#### Square関連
- **"Invalid access token"**
  - SQUARE_ACCESS_TOKENが正しく設定されているか確認
  - アクセストークンが有効か確認

## ■ セキュリティ注意事項

1. **.envファイルはGitにコミットしない**
   - .gitignoreに.envを追加
   - 本番環境では環境変数として設定

2. **認証ファイルの管理**
   - サービスアカウントキーは安全に保管
   - 定期的にローテーション

3. **API制限の確認**
   - 各APIのレート制限を確認
   - 必要に応じてリトライ処理を実装

## ■ 本実装への移行手順

1. **テストデータで動作確認**
   ```bash
   python scripts/generate_test_data.py
   streamlit run streamlit_app.py
   ```

2. **環境変数を設定**
   - 上記の手順で.envファイルを作成・編集

3. **接続テスト実行**
   ```bash
   python scripts/run_etl.py
   ```

4. **本実装データで動作確認**
   ```bash
   streamlit run streamlit_app.py
   ```

---

**作成日**: 2025-09-02  
**バージョン**: 1.0.0  
**作成者**: Cursor AI Assistant
