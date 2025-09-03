# Google Analytics 4 データ取得スクリプト

このスクリプトは、Google Analytics 4のデータをOAuth 2.0認証を使用して取得し、CSVファイルとして出力するPythonスクリプトです。

## 機能

- OAuth 2.0認証によるGoogle Analytics APIへの安全なアクセス
- 直近30日間のデータ取得
- セッション数と総収益の取得
- 日付とソース別のディメンション分析
- Pandas DataFrameへの変換
- CSVファイル出力
- 基本統計情報の表示

## セットアップ

### 1. 必要なライブラリのインストール

```bash
pip install -r requirements.txt
```

または個別にインストール：

```bash
pip install google-analytics-data google-auth google-auth-oauthlib google-auth-httplib2 pandas
```

### 2. Shopify Admin APIの設定

1. [Shopify管理画面](https://admin.shopify.com/)にアクセス
2. 設定 > アプリとチャネル > プライベートアプリ に移動
3. 新しいプライベートアプリを作成
4. Admin API統合を有効化
5. 必要な権限を設定（注文読み取り、商品読み取り）
6. APIトークンをコピー

環境変数の設定:
```bash
# Windows PowerShell
$env:SHOPIFY_API_TOKEN="your_api_token_here"

# Windows Command Prompt
set SHOPIFY_API_TOKEN=your_api_token_here

# または、.envファイルを作成（推奨）
echo SHOPIFY_API_TOKEN=your_api_token_here > .env
```

### 3. Google Analytics APIの設定

1. [Google Cloud Console](https://console.cloud.google.com/)にアクセス
2. 新しいプロジェクトを作成または既存のプロジェクトを選択
3. Google Analytics Data API v1を有効化
4. OAuth 2.0クライアントIDを作成
5. 認証情報ファイル（JSON）をダウンロードし、プロジェクトルートに配置

### 3. 設定の確認

スクリプト内の以下の設定を確認・変更してください：

```python
GA4_PROPERTY_ID = '315830165'  # あなたのGA4プロパティID
CREDENTIALS_FILE = 'client_secret_*.json'  # 認証情報ファイル名
```

## 使用方法

### 1. Shopifyデータ取得

```bash
python shopify_data_extractor.py
```

### 2. Google Analyticsデータ取得

### 基本的な実行

```bash
python ga4_data_extractor.py
```

### 3. データ統合・分析

```bash
python data_analyzer.py
```

### 4. 戦略提案

```bash
python strategy_proposer.py
```

### 5. 全パイプライン実行（推奨）

```bash
python run_analysis_pipeline.py
```

### 初回実行時

初回実行時は、ブラウザが開いてGoogleアカウントでの認証が求められます：

1. スクリプトを実行
2. ブラウザが自動で開く
3. Googleアカウントでログイン
4. 必要な権限を承認
5. 認証完了後、トークンが`token.pickle`ファイルに保存される

### 2回目以降の実行

認証トークンが保存されているため、再認証は不要です。

## 出力ファイル

スクリプト実行後、以下のファイルが生成されます：

- `ga4_data_YYYY-MM-DD_to_YYYY-MM-DD.csv`: 取得したデータのCSVファイル
- `token.pickle`: 認証トークン（自動生成）

## 取得データ

### 指標（Metrics）
- `sessions`: セッション数
- `totalRevenue`: 総収益

### ディメンション（Dimensions）
- `date`: 日付
- `source`: トラフィックソース

## エラーハンドリング

スクリプトは以下のエラーに対応しています：

- 認証エラー
- API接続エラー
- データ取得エラー
- ファイル出力エラー

## トラブルシューティング

### よくある問題

1. **認証エラー**
   - 認証情報ファイルが正しく配置されているか確認
   - Google Cloud ConsoleでAPIが有効化されているか確認

2. **権限エラー**
   - GA4プロパティへのアクセス権限があるか確認
   - プロパティIDが正しいか確認

3. **ライブラリエラー**
   - 必要なライブラリがインストールされているか確認
   - `pip install -r requirements.txt`を実行

### ログの確認

スクリプト実行時に詳細なログが表示されます。エラーが発生した場合は、表示されるメッセージを確認してください。

## カスタマイズ

### 期間の変更

```python
# 直近7日間の場合
start_date = end_date - timedelta(days=7)

# 特定の期間の場合
start_date = datetime(2024, 1, 1).date()
end_date = datetime(2024, 1, 31).date()
```

### 指標・ディメンションの追加

```python
metrics=[
    Metric(name="sessions"),
    Metric(name="totalRevenue"),
    Metric(name="users"),  # 追加
    Metric(name="pageViews")  # 追加
],
dimensions=[
    Dimension(name="date"),
    Dimension(name="source"),
    Dimension(name="medium"),  # 追加
    Dimension(name="campaign")  # 追加
]
```

## セキュリティ

- 認証トークンは`token.pickle`ファイルに暗号化して保存
- OAuth 2.0による安全な認証
- 読み取り専用権限のみを使用

## ライセンス

このスクリプトはMITライセンスの下で提供されています。
