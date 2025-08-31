# 🚀 Shopify x GA4 x Square x Google Ads 統合ダッシュボード

## 📋 プロジェクト概要

**Shopify x GA4 x Square x Google Ads 統合ダッシュボード**は、ECサイトのマーケティング効果を包括的に測定・分析するための統合システムです。

### 🎯 目的
- Shopify、Google Analytics 4 (GA4)、Square POS、Google Adsの4つのデータソースを統合
- 売上分析、顧客行動分析、マーケティングROI測定を一元化
- データドリブンな意思決定支援システムの構築

### 🔗 統合データソース
- **Shopify**: ECサイトの売上・商品データ
- **Google Analytics 4**: ウェブサイトのトラフィック・行動データ
- **Square POS**: 実店舗の決済データ
- **Google Ads**: 広告効果・ROASデータ

## 🛠️ 技術スタック

### 開発環境
- **OS**: Windows 10/11
- **Python**: 3.13.3
- **パッケージ管理**: pip
- **バージョン管理**: Git

### フロントエンド・UI
- **Streamlit**: 1.37.1 - メインダッシュボードフレームワーク
- **Plotly**: 5.23.0 - インタラクティブチャート・グラフ
- **Altair**: 5.5.0 - データ可視化（Google Ads用）

### データ処理・分析
- **Pandas**: 2.2.3 - データ操作・分析
- **NumPy**: 2.2.2 - 数値計算
- **Pydantic**: 2.10.6 - データバリデーション・スキーマ定義

### API統合
- **Google Analytics Data API**: google-analytics-data 0.18.0
- **Google Ads API**: google-ads 24.1.0
- **Shopify Admin API**: ShopifyAPI 12.3.0
- **Square Payments API**: squareup 40.1.0.220250220

## 📁 プロジェクト構造

```
my-shopify-ga-app/
├── 📁 config/                    # 設定ファイル
│   ├── requirements.txt          # Python依存関係
│   └── google_ads.yaml           # Google Ads設定
├── 📁 data/                      # データフォルダ
│   ├── 📁 raw/                   # 生データ（CSV、認証ファイル）
│   ├── 📁 processed/             # 処理済みデータ
│   ├── 📁 reports/              # 分析レポート
│   └── 📁 ads/                   # Google Ads関連データ
│       ├── 📁 raw/               # 生データ
│       ├── 📁 processed/         # 処理済み
│       └── 📁 cache/             # キャッシュ・フィクスチャ
├── 📁 src/                       # ソースコード
│   ├── 📁 extractors/            # データ抽出
│   ├── 📁 analysis/              # データ分析
│   ├── 📁 ads/                   # Google Ads統合
│   ├── 📁 ga4/                   # GA4関連
│   ├── 📁 shopify/               # Shopify関連
│   ├── 📁 ui/                    # UI関連
│   └── 📁 utils/                 # ユーティリティ
├── 📁 docs/                      # ドキュメント
├── 📁 logs/                      # ログファイル
├── 📁 app_tabs/                  # Streamlitタブ
├── streamlit_app.py              # メインアプリ
└── README.md                     # このファイル
```

## 🚀 セットアップ

### 1. 前提条件
- Python 3.13.3以上
- Git
- 各プラットフォームのAPI認証情報

### 2. リポジトリクローン
```bash
git clone https://github.com/botarhythm/my-shopify-ga-app.git
cd my-shopify-ga-app
```

### 3. 依存関係インストール
```bash
pip install -r config/requirements.txt
```

### 4. 環境変数設定
`.env`ファイルを作成し、以下の認証情報を設定：

```env
# Google Analytics 4
GA4_PROPERTY_ID=your_ga4_property_id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service_account.json

# Shopify
SHOPIFY_SHOP_URL=your_shop_url
SHOPIFY_ACCESS_TOKEN=your_access_token

# Square
SQUARE_ACCESS_TOKEN=your_square_access_token
SQUARE_LOCATION_ID=your_location_id

# Google Ads
GOOGLE_ADS_CLIENT_ID=your_client_id
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_CUSTOMER_ID=your_customer_id
```

### 5. フィクスチャデータ生成（テスト用）
```bash
python src/ads/generate_fixtures.py --start 2025-08-01 --end 2025-08-30
```

### 6. アプリケーション起動
```bash
streamlit run streamlit_app.py
```

**アクセスURL**: http://localhost:8504

## 📊 主要機能

### 1. 統合KPIダッシュボード
- **総売上**: Shopify + Squareの統合売上
- **セッション数**: GA4からのトラフィックデータ
- **売上/セッション比率**: コンバージョン効率
- **昨年同期対比**: YoY成長率分析

### 2. 詳細分析機能
- **商品別売上ランキング**: トップ商品の特定
- **流入元別エンゲージメント**: チャネル効果測定
- **コンテンツパフォーマンス**: ページ別分析
- **SEO効果分析**: 検索流入の最適化

### 3. Google Ads分析
- **キャンペーン別ROAS**: 広告効果測定
- **キーワード別パフォーマンス**: 検索キーワード分析
- **GA4ブリッジ分析**: 広告→コンバージョン追跡
- **日別パフォーマンス推移**: 時系列分析

### 4. 自動レポート生成
- **定期分析レポート**: 自動生成・保存
- **クロス分析**: 複数データソース統合分析
- **戦略提案**: AI支援の改善提案

## 🔐 セキュリティ

### 認証情報管理
- **OAuth 2.0**: Google APIs用
- **API Token**: Shopify、Square用
- **環境変数**: 認証情報の安全な管理
- **`.gitignore`**: 認証ファイルの除外設定

### データ保護
- **Privateリポジトリ**: セキュリティ強化
- **認証情報除外**: Git履歴からの完全削除
- **環境変数**: 本番環境での安全な管理

## 📈 データフロー

```
1. データ抽出 (src/extractors/)
   ↓
2. データ処理 (src/analysis/)
   ↓
3. データ可視化 (Streamlit)
   ↓
4. レポート生成 (data/reports/)
```

## 🧪 テスト

### フィクスチャデータ
- **期間**: 2025-08-01 〜 2025-08-30（30日間）
- **データ量**: 
  - キャンペーン: 240行
  - キーワード: 720行
  - ロールアップ: 240行

### テスト実行
```bash
# フィクスチャデータ生成
python src/ads/generate_fixtures.py --start 2025-08-01 --end 2025-08-30

# 分析パイプライン実行
python src/analysis/run_analysis_pipeline.py
```

## 📝 開発ガイド

### 新しいデータソース追加
1. `src/extractors/`に新しい抽出スクリプトを作成
2. `src/analysis/`に分析ロジックを追加
3. `streamlit_app.py`にUIタブを追加

### 新しい分析機能追加
1. `src/analysis/`に分析スクリプトを作成
2. `app_tabs/`にUIコンポーネントを追加
3. メインアプリに統合

## 🚧 制限事項・注意事項

### データファイル
- **実際のデータファイル**: Gitにアップロードしない
- **認証情報**: 環境変数で管理
- **個人情報**: 適切に匿名化・暗号化

### API制限
- **Google Ads API**: Basic Access承認が必要
- **レート制限**: 各APIの制限に注意
- **データ更新**: リアルタイムではない

## 🤝 貢献

1. このリポジトリをフォーク
2. 機能ブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. プルリクエストを作成

## 📄 ライセンス

このプロジェクトはPrivateリポジトリです。

## 📞 サポート

- **Issues**: GitHub Issuesでバグ報告・機能要望
- **Documentation**: `docs/`フォルダ内の詳細ドキュメント
- **Project Milestones**: `PROJECT_MILESTONES.md`で進捗確認

---

**最終更新**: 2025年1月
**プロジェクトステータス**: フィクスチャデータテスト完了、API統合準備完了
