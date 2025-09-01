# 環境変数設定ガイド

## 概要

Google Ads APIを使用するために必要な環境変数の設定方法について説明します。MCCベーシックアカウントでも同じ認証情報が必要です。

## 必要な環境変数

以下の5つの環境変数が必要です：

```bash
GOOGLE_ADS_CLIENT_ID=your_client_id
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_CUSTOMER_ID=your_customer_id
```

## 環境変数の取得方法

### 1. GOOGLE_ADS_CLIENT_ID と GOOGLE_ADS_CLIENT_SECRET

**Google Cloud Console から取得**:

1. [Google Cloud Console](https://console.cloud.google.com/) にアクセス
2. プロジェクトを選択（または新規作成）
3. 「APIとサービス」→「認証情報」を選択
4. 「認証情報を作成」→「OAuth 2.0 クライアントID」を選択
5. アプリケーションの種類を選択（デスクトップアプリケーション推奨）
6. クライアントIDとクライアントシークレットをコピー

### 2. GOOGLE_ADS_REFRESH_TOKEN

**OAuth認証フローから取得**:

1. Google Ads APIのOAuth認証を実行
2. 認証コードを取得
3. リフレッシュトークンを取得

**注意**: リフレッシュトークンは一度だけ表示されるため、安全に保存してください。

### 3. GOOGLE_ADS_DEVELOPER_TOKEN

**Google Ads アカウントから取得**:

1. [Google Ads](https://ads.google.com/) にログイン
2. ツールと設定 → 設定 → APIセンター
3. 開発者トークンをコピー

**注意**: 開発者トークンの取得には審査が必要な場合があります。

### 4. GOOGLE_ADS_CUSTOMER_ID

**Google Ads アカウントから取得**:

1. Google Adsにログイン
2. 右上のアカウント番号をコピー（例：123-456-7890）
3. ハイフンを除去して10桁の数字に変換（例：1234567890）

## Windows環境での設定方法

### 方法1: PowerShell（推奨）

**一時的な設定（現在のセッションのみ）**:

```powershell
# 現在のセッションに環境変数を設定
$env:GOOGLE_ADS_CLIENT_ID = "your_client_id"
$env:GOOGLE_ADS_CLIENT_SECRET = "your_client_secret"
$env:GOOGLE_ADS_REFRESH_TOKEN = "your_refresh_token"
$env:GOOGLE_ADS_DEVELOPER_TOKEN = "your_developer_token"
$env:GOOGLE_ADS_CUSTOMER_ID = "your_customer_id"

# 設定確認
Get-ChildItem Env:GOOGLE_ADS_*
```

**永続的な設定（システム全体）**:

```powershell
# システム環境変数として設定
[Environment]::SetEnvironmentVariable("GOOGLE_ADS_CLIENT_ID", "your_client_id", "User")
[Environment]::SetEnvironmentVariable("GOOGLE_ADS_CLIENT_SECRET", "your_client_secret", "User")
[Environment]::SetEnvironmentVariable("GOOGLE_ADS_REFRESH_TOKEN", "your_refresh_token", "User")
[Environment]::SetEnvironmentVariable("GOOGLE_ADS_DEVELOPER_TOKEN", "your_developer_token", "User")
[Environment]::SetEnvironmentVariable("GOOGLE_ADS_CUSTOMER_ID", "your_customer_id", "User")

# 設定確認
[Environment]::GetEnvironmentVariable("GOOGLE_ADS_CLIENT_ID", "User")
```

### 方法2: コマンドプロンプト

**一時的な設定**:

```cmd
set GOOGLE_ADS_CLIENT_ID=your_client_id
set GOOGLE_ADS_CLIENT_SECRET=your_client_secret
set GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
set GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
set GOOGLE_ADS_CUSTOMER_ID=your_customer_id

# 設定確認
echo %GOOGLE_ADS_CLIENT_ID%
```

**永続的な設定**:

```cmd
setx GOOGLE_ADS_CLIENT_ID "your_client_id"
setx GOOGLE_ADS_CLIENT_SECRET "your_client_secret"
setx GOOGLE_ADS_REFRESH_TOKEN "your_refresh_token"
setx GOOGLE_ADS_DEVELOPER_TOKEN "your_developer_token"
setx GOOGLE_ADS_CUSTOMER_ID "your_customer_id"
```

### 方法3: Windows設定画面

1. **Windowsキー + R** を押して「sysdm.cpl」と入力
2. 「詳細設定」タブ → 「環境変数」ボタンをクリック
3. 「ユーザー環境変数」セクションで「新規」をクリック
4. 各環境変数を個別に追加

## 設定確認方法

### PowerShellでの確認

```powershell
# 全Google Ads関連環境変数を表示
Get-ChildItem Env:GOOGLE_ADS_*

# 個別確認
echo "Client ID: $env:GOOGLE_ADS_CLIENT_ID"
echo "Developer Token: $env:GOOGLE_ADS_DEVELOPER_TOKEN"
echo "Customer ID: $env:GOOGLE_ADS_CUSTOMER_ID"
```

### Pythonでの確認

```python
import os

# 環境変数の確認
required_vars = [
    "GOOGLE_ADS_CLIENT_ID",
    "GOOGLE_ADS_CLIENT_SECRET", 
    "GOOGLE_ADS_REFRESH_TOKEN",
    "GOOGLE_ADS_DEVELOPER_TOKEN",
    "GOOGLE_ADS_CUSTOMER_ID"
]

for var in required_vars:
    value = os.getenv(var)
    if value:
        print(f"✅ {var}: {value[:10]}...")  # 最初の10文字のみ表示
    else:
        print(f"❌ {var}: 未設定")
```

## セキュリティのベストプラクティス

### 1. 環境変数の保護

- **リフレッシュトークン**: 最も重要な認証情報、安全に保管
- **クライアントシークレット**: 機密情報、共有しない
- **開発者トークン**: アカウント固有、適切に管理

### 2. 開発環境での注意点

```bash
# .envファイルを使用する場合（推奨）
# .envファイルを.gitignoreに追加
echo ".env" >> .gitignore

# .envファイルの例
GOOGLE_ADS_CLIENT_ID=your_client_id
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_CUSTOMER_ID=your_customer_id
```

### 3. 本番環境での管理

- 環境変数管理ツールの使用（例：Azure Key Vault、AWS Secrets Manager）
- 定期的なローテーション
- アクセス権限の最小化

## トラブルシューティング

### よくある問題

1. **環境変数が認識されない**
   - 新しいターミナルセッションを開始
   - システム環境変数の場合は再起動が必要

2. **権限エラー**
   - 管理者権限でPowerShellを実行
   - ユーザー環境変数として設定

3. **文字化け**
   - UTF-8エンコーディングで設定
   - 特殊文字のエスケープ

### デバッグ方法

```powershell
# 環境変数の詳細確認
Get-ChildItem Env: | Where-Object {$_.Name -like "*GOOGLE_ADS*"}

# 環境変数のパス確認
$env:PATH -split ';' | Where-Object {$_ -like "*Google*"}
```

## 設定後のテスト

### 1. 基本的な接続テスト

```python
# src/ads/google_ads_client.py を実行
python src/ads/google_ads_client.py
```

### 2. データ取得テスト

```python
# 簡単なデータ取得テスト
from src.ads.fetch_ads import GoogleAdsDataFetcher

fetcher = GoogleAdsDataFetcher()
# エラーが発生しなければ環境変数は正しく設定されています
```

## まとめ

環境変数の設定は、Google Ads APIを使用するための重要な第一歩です。適切に設定することで、MCCベーシックアカウントでも安定したAPI接続が可能になります。

**設定のポイント**:
1. 5つの必須環境変数を漏れなく設定
2. セキュリティを考慮した管理
3. 設定後の動作確認
4. 必要に応じて永続化

設定に問題がある場合は、上記のトラブルシューティング手順を参考にしてください。
