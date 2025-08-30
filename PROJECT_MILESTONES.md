# 🚀 Shopify x GA4 x Square x Google Ads 統合ダッシュボード プロジェクト

## 📋 プロジェクト概要

**目的**: マーケティング効果測定システムの構築
**統合データソース**: Shopify、GA4、Square、Google Ads
**フレームワーク**: Streamlit（Python）
**現在のステータス**: フィクスチャデータでのテスト完了

## 🎯 主要マイルストーン

### ✅ 完了済み

#### 1. プロジェクト基盤構築
- [x] プロジェクト構造の設計
- [x] 依存関係の管理（requirements.txt）
- [x] データディレクトリ構造の作成
- [x] 基本的なStreamlitアプリの構築

#### 2. データ統合基盤
- [x] Shopifyデータ抽出機能
- [x] GA4データ抽出機能
- [x] Square決済データ抽出機能
- [x] CSV自動検出・読み込み機能

#### 3. ダッシュボード機能
- [x] 統合KPI表示（売上、セッション、決済）
- [x] 昨年同期対比分析
- [x] 時系列チャート（売上、セッション推移）
- [x] 商品別売上分析
- [x] 流入元分析
- [x] コンテンツ・SEO分析
- [x] タブ構造の実装

#### 4. Google Ads API統合準備
- [x] Google Ads API依存関係の解決
- [x] OAuth 2.0認証設定
- [x] フィクスチャデータ生成機能
- [x] Google Adsタブの実装
- [x] キャンペーン分析機能
- [x] キーワード分析機能
- [x] GA4ブリッジ分析機能

#### 5. 技術的課題解決
- [x] protobuf依存関係の競合解決
- [x] google-adsライブラリのバージョン互換性確保
- [x] OAuth 2.0スコープの正しい設定
- [x] フィクスチャデータでのUI/UXテスト

## 🔧 技術仕様

### 使用技術
- **フレームワーク**: Streamlit 1.37.1
- **データ処理**: Pandas 2.2.3
- **可視化**: Plotly 5.23.0
- **Google Ads API**: google-ads 28.0.0
- **認証**: OAuth 2.0
- **データ形式**: CSV、Parquet

### プロジェクト構造
```
my-shopify-ga-app/
├── config/
│   ├── requirements.txt
│   └── google_ads.yaml
├── data/
│   ├── raw/          # 生データ（CSV）
│   ├── processed/    # 処理済みデータ
│   ├── ads/
│   │   └── cache/    # Google Adsフィクスチャデータ
│   └── reports/      # 分析レポート
├── src/
│   ├── extractors/   # データ抽出スクリプト
│   ├── ads/          # Google Ads関連
│   ├── analysis/     # 分析スクリプト
│   └── utils/        # ユーティリティ
├── docs/             # ドキュメント
├── logs/             # ログファイル
└── streamlit_app.py  # メインアプリ
```

## 📊 現在の機能

### 統合KPIダッシュボード
- 総売上（Shopify + Square）
- セッション数
- 売上/セッション比率
- 昨年同期対比

### 詳細分析機能
- 商品別売上ランキング
- 流入元別エンゲージメント分析
- コンテンツパフォーマンス分析
- SEO効果分析

### Google Ads分析（フィクスチャデータ）
- キャンペーン別広告費・ROAS
- キーワード別パフォーマンス
- GA4ブリッジ分析
- 日別パフォーマンス推移

## 🔐 認証設定

### Google Ads API認証情報
- **Customer ID**: 82314524
- **Client ID**: 159450887000-7ic0t1o3jef858l192rodo6fju1b62qf.apps.googleusercontent.com
- **Developer Token**: 申請済み（Basic Access待ち）
- **OAuth 2.0**: 設定完了（adwords + analytics.readonly スコープ）

### その他のAPI
- **Shopify API**: 設定済み
- **Square API**: 設定済み
- **GA4**: 設定済み

## 🚧 進行中・未完了

### 1. Google Ads API Basic Access承認
- **ステータス**: 申請済み、承認待ち
- **影響**: 現在はフィクスチャデータでテスト中
- **次のアクション**: 承認後、実際のAPIデータに切り替え

### 2. 機能拡張予定
- [ ] より詳細なROAS分析
- [ ] 自動レポート生成
- [ ] アラート機能
- [ ] データ品質監視
- [ ] パフォーマンス最適化

## 📈 フィクスチャデータ仕様

### 生成期間
- **開始日**: 2025-08-01
- **終了日**: 2025-08-30
- **データ量**: 30日間

### データセット
- **campaign**: 240行, 16列
- **ad_group**: 広告グループデータ
- **keyword**: 720行, 16列
- **ga4_bridge**: 120行, 10列
- **shopify_sales**: 120行, 5列
- **rollup**: 240行, 16列

### キャンペーン例
1. チオピア豆プロモーション
2. コーヒー豆ブランド認知
3. ニカラグア豆セール
4. ブランド名検索
5. ブランド検索
6. リターゲティング
7. 季節商品キャンペーン
8. 新商品ローンチ

## 🛠️ 開発環境

### 実行方法
```bash
# 依存関係インストール
pip install -r config/requirements.txt

# フィクスチャデータ生成
python src/ads/generate_fixtures.py --start 2025-08-01 --end 2025-08-30

# Streamlitアプリ起動
streamlit run streamlit_app.py
```

### アクセスURL
- **ローカル**: http://localhost:8504
- **ネットワーク**: http://192.168.11.13:8504

## 📝 注意事項

### セキュリティ
- `.env`ファイルは`.gitignore`に含める
- API認証情報は環境変数で管理
- 本番環境では適切なセキュリティ設定が必要

### データ管理
- 生データは`data/raw/`に配置
- 処理済みデータは`data/processed/`に保存
- フィクスチャデータは`data/ads/cache/`に保存

## 🎯 次のステップ

1. **Google Ads API Basic Access承認待ち**
2. **実際のAPIデータ統合**
3. **パフォーマンス最適化**
4. **追加分析機能の実装**
5. **本番環境デプロイ準備**

---

**最終更新**: 2025年1月
**プロジェクトステータス**: フィクスチャデータテスト完了、API統合準備完了
