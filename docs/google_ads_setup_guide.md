# Google Ads API 環境変数設定ガイド

## 🔑 必要な環境変数

### 1. GOOGLE_ADS_CLIENT_ID
- **取得方法**: Google Cloud Console → APIs & Services → Credentials
- **形式**: `123456789-abcdefghijklmnop.apps.googleusercontent.com`
- **説明**: OAuth 2.0クライアントID

### 2. GOOGLE_ADS_CLIENT_SECRET  
- **取得方法**: Google Cloud Console → APIs & Services → Credentials
- **形式**: `GOCSPX-abcdefghijklmnopqrstuvwxyz`
- **説明**: OAuth 2.0クライアントシークレット

### 3. GOOGLE_ADS_REFRESH_TOKEN
- **取得方法**: OAuth認証フロー実行後に取得
- **形式**: `1//04abcdefghijklmnopqrstuvwxyz`
- **説明**: アクセストークンの更新用トークン

### 4. GOOGLE_ADS_DEVELOPER_TOKEN
- **取得方法**: Google Ads → Tools & Settings → API Center
- **形式**: `abcdefghijklmnop`
- **説明**: Google Ads API開発者トークン

### 5. GOOGLE_ADS_CUSTOMER_ID
- **取得方法**: Google Ads → アカウント設定
- **形式**: `1234567890`
- **説明**: 広告アカウントの顧客ID

## 🚀 設定手順

### Step 1: Google Cloud Console設定
1. https://console.cloud.google.com/ にアクセス
2. プロジェクトを作成または選択
3. Google Ads APIを有効化
4. OAuth 2.0クライアントIDを作成

### Step 2: Google Ads API Center設定
1. https://ads.google.com/ にアクセス
2. Tools & Settings → API Center
3. 開発者トークンを申請・取得

### Step 3: OAuth認証フロー実行
```bash
# OAuth認証スクリプトを実行
python src/ads/oauth_setup.py
```

### Step 4: 環境変数設定
```bash
# .envファイルに追加
GOOGLE_ADS_CLIENT_ID=your_client_id
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_CUSTOMER_ID=your_customer_id
```

## 🔧 MCCベーシックアカウント対応

### 制限事項
- 一部の高度なAPI機能が制限される
- リクエスト頻度に制限がある
- 一部のレポートが利用できない

### 対応策
- 基本的なメトリクスのみ取得
- 保守的なリクエスト頻度設定
- エラーハンドリングの強化

## 📊 取得可能なデータ

### キャンペーンデータ
- インプレッション数
- クリック数
- CTR (クリック率)
- コスト
- コンバージョン数
- ROAS (投資対効果)

### 広告グループデータ
- 広告グループ別パフォーマンス
- キーワード別パフォーマンス
- 入札戦略

### 時間別データ
- 日別パフォーマンス
- 時間帯別パフォーマンス
- 曜日別パフォーマンス

## 🚨 よくある問題と解決策

### 1. 認証エラー
**問題**: "Invalid credentials" エラー
**解決策**: リフレッシュトークンの再取得

### 2. 権限エラー  
**問題**: "Access denied" エラー
**解決策**: Google Adsアカウントの権限確認

### 3. レート制限エラー
**問題**: "Rate limit exceeded" エラー
**解決策**: リクエスト頻度の調整

### 4. MCC制限エラー
**問題**: "Feature not available" エラー
**解決策**: 基本機能のみ使用