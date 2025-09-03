# MCCベーシックアカウント対応ガイド

## 概要

Google Ads MCCアカウントがベーシックに切り替わった際の対応策と設定変更について説明します。

## MCCベーシック vs 標準MCCの違い

### MCCベーシックの制限事項

1. **API機能制限**
   - 一部の高度な機能が制限される場合があります
   - APIリクエストの頻度制限がより厳しくなる可能性があります

2. **管理機能制限**
   - 複数アカウントの一括操作に制限がかかる場合があります
   - レポート機能の一部が制限される場合があります

## 実装した対応策

### 1. 設定ファイルの更新

`config/google_ads.yaml`に以下の設定を追加：

```yaml
api:
  # MCC account type (basic/standard)
  mcc_account_type: "basic"
  
  # API access level restrictions for basic MCC
  basic_mcc_restrictions:
    # Basic MCCでは一部の高度な機能が制限される
    advanced_features_disabled: true
    # APIリクエスト頻度制限
    rate_limit_conservative: true
```

### 2. クライアント機能の拡張

`GoogleAdsClientFactory`に以下の機能を追加：

- `is_basic_mcc()`: MCCベーシックアカウントかどうかの判定
- `get_mcc_restrictions()`: MCCベーシックの制限設定取得

### 3. データ取得機能の調整

`GoogleAdsDataFetcher`に以下の対応を実装：

- **動的リトライ戦略**: MCCベーシックの場合、より控えめなリトライ設定
- **レート制限対応**: リクエスト前の待機時間追加
- **ログ出力強化**: MCCベーシック対応の詳細ログ

## 使用方法

### 1. 環境変数の確認

MCCベーシックでも同じ認証情報が必要です：

```bash
GOOGLE_ADS_CLIENT_ID=your_client_id
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_CUSTOMER_ID=your_customer_id
```

### 2. 設定の確認

`config/google_ads.yaml`でMCCアカウントタイプを設定：

```yaml
api:
  mcc_account_type: "basic"  # "basic" または "standard"
```

### 3. データ取得テスト

```python
from src.ads.fetch_ads import GoogleAdsDataFetcher

fetcher = GoogleAdsDataFetcher()
# MCCベーシック対応のログが出力されます
campaign_data = fetcher.fetch_campaign_data("2024-01-01", "2024-01-31")
```

## トラブルシューティング

### 1. 認証エラー

**症状**: "Missing required Google Ads credentials"

**対処法**:
- 環境変数が正しく設定されているか確認
- MCCベーシックでも同じ認証情報が必要

### 2. APIレート制限エラー

**症状**: Rate limit exceeded エラー

**対処法**:
- `rate_limit_conservative: true`が設定されているか確認
- リクエスト間隔を調整

### 3. 機能制限エラー

**症状**: 特定のAPI機能が使用できない

**対処法**:
- `advanced_features_disabled: true`を確認
- 代替の基本機能を使用

## 影響を受ける機能

### 制限される可能性のある機能

1. **高度なレポート機能**
   - カスタムレポートの一部
   - 複雑なセグメント分析

2. **一括操作**
   - 複数アカウントの同時操作
   - 大量データの一括処理

3. **リアルタイム機能**
   - 頻繁なデータ更新
   - リアルタイム監視

### 引き続き使用可能な機能

1. **基本データ取得**
   - キャンペーン、広告グループ、キーワードデータ
   - 標準メトリクス（クリック、インプレッション、コンバージョン）

2. **レポート生成**
   - 日次、週次、月次レポート
   - 基本的な分析とグラフ

3. **データ統合**
   - GA4、Shopify、Squareとの統合
   - クロス分析機能

## 今後の対応

### 1. モニタリング

- APIエラー率の監視
- レスポンス時間の追跡
- 制限に関するログの確認

### 2. 最適化

- クエリの効率化
- キャッシュ戦略の見直し
- バッチ処理の最適化

### 3. アップグレード検討

必要に応じてMCC標準アカウントへのアップグレードを検討：

1. **使用量の評価**
   - 現在の機能使用状況
   - 制限による影響度

2. **コスト対効果**
   - アップグレード費用
   - 業務効率の向上度

3. **代替案の検討**
   - 他のツールとの組み合わせ
   - ワークフローの変更

## 結論

MCCベーシックアカウントでも、適切な設定と対応により、主要な機能は継続して使用可能です。制限事項を理解し、必要に応じて運用方法を調整することで、効果的な広告分析を継続できます。
