# Shopify GA4 統合アプリ 引継ぎプロンプト

## プロジェクト概要
Shopify、Google Analytics 4、Google Ads、Squareのデータを統合したダッシュボードアプリケーション。DuckDBを使用したデータウェアハウスとStreamlitによる可視化。

## 現在の状況（2025-09-03）

### ✅ 完了済み
1. **プロジェクト構造**: 完全に構築済み
2. **API認証**: GA4（OAuth 2.0）、Google Ads、Shopify、Square
3. **データベース設計**: DuckDBスキーマ（core_*、mart_*、mart_daily_yoy）
4. **ETLパイプライン**: 完全に動作中
5. **Streamlit UI**: ダッシュボード本格運用中
6. **エラーハンドリング**: 完全実装済み
7. **実データ統合**: 全APIからの実データ取得完了
8. **データ品質管理**: 監視・チェック機能実装済み
9. **統合分析**: 2025年8月データの完全統合完了

### ✅ 解決済みの問題

#### 1. カラム数不一致エラー
**エラー**: `Binder Error: table core_shopify has 10 columns but 14 values were supplied`
**エラー**: `Binder Error: table core_square has 6 columns but 10 values were supplied`

**解決済み**: 
- `scripts/bootstrap.sql`: Shopifyテーブルを16カラムに拡張
- `src/connectors/shopify.py`: カラム名と順序を調整
- `scripts/bootstrap.sql`: Squareテーブルを15カラムに拡張
- `src/connectors/square.py`: カラム名と順序を調整、dateカラム追加

#### 2. UnicodeDecodeError
**エラー**: `UnicodeDecodeError: 'utf-8' codec can't decode byte 0x8a in position 0: invalid start byte`

**解決済み**: 
- `scripts/bootstrap_duckdb.py`: 絵文字を削除
- `scripts/run_etl.py`: 絵文字を削除

#### 3. DuckDBファイル破損
**エラー**: `UnicodeDecodeError: 'utf-8' codec can't decode byte 0x83 in position 110: invalid start byte`

**解決済み**: 
- ファイル名を`commerce_test.duckdb`に変更
- 全スクリプトのDBパスを更新済み

#### 4. GitHub Push Protection
**エラー**: 機密情報（API トークン、OAuth認証情報）が含まれているためプッシュ拒否

**解決済み**: 
- `.env.backup`、`ga4_token.pickle`、DBファイルを削除済み
- 機密情報を完全に除外した状態でGitHub同期可能

## 技術スタック
- **データベース**: DuckDB
- **フロントエンド**: Streamlit
- **API**: Google Analytics 4, Google Ads, Shopify, Square
- **認証**: OAuth 2.0 (GA4), API Tokens (Shopify, Square)
- **言語**: Python 3.13

## 重要なファイル構成

### コアファイル
- `streamlit_app.py`: メインアプリケーション
- `scripts/bootstrap.sql`: データベーススキーマ定義
- `scripts/run_etl.py`: ETLパイプライン
- `scripts/bootstrap_duckdb.py`: データベース初期化
- `test_local.py`: ローカルテストスクリプト

### コネクタ
- `src/connectors/ga4.py`: Google Analytics 4
- `src/connectors/google_ads.py`: Google Ads
- `src/connectors/shopify.py`: Shopify
- `src/connectors/square.py`: Square

### 設定
- `.env`: 環境変数（機密情報）
- `config/`: 設定ファイル

## 現在の運用状況

### 1. データ統合状況
- **Shopify**: 528レコード（2025年8月の注文データ）
- **Square**: 79レコード（2025年8月の支払いデータ）
- **GA4**: 844セッション（2025年8月のトラフィックデータ）
- **Google Ads**: 85,768インプレッション（2025年8月の広告データ）

### 2. 統合分析結果
- **総売上**: ¥273,540（Shopify ¥60,610 + Square ¥212,930）
- **総取引数**: 93件（Shopify 14件 + Square 79件）
- **トラフィック**: GA4セッション844、ユーザー763
- **広告効果**: Google Ads費用¥6,102、ROAS 44.83

### 3. システム運用
- **データベース**: DuckDB正常動作中
- **ETLパイプライン**: 完全動作中
- **Streamlitダッシュボード**: 本格運用中
- **データ品質監視**: 自動チェック機能動作中

## 次のステップ

### 1. 継続運用
1. **日次データ更新**
   ```bash
   python scripts/run_etl.py
   ```

2. **Streamlitアプリ起動**
   ```bash
   streamlit run streamlit_app.py
   ```

3. **データ品質チェック**
   ```bash
   python scripts/health_check.py
   ```

### 2. 機能拡張
1. **自動レポート生成**: メール配信機能
2. **アラート機能**: 異常値検出・通知
3. **リアルタイム更新**: データの即時反映
4. **モバイル対応**: レスポンシブUI
5. **予測分析**: 売上予測機能

### 3. GitHub同期
- 機密情報を完全に除外した状態でGitHub同期可能
- 新しいリポジトリまたは既存リポジトリへの安全なプッシュ

## 環境変数設定
```bash
# .env ファイルに以下を設定
DUCKDB_PATH=./data/duckdb/commerce_test.duckdb
GA4_PROPERTY_ID=315830165
GOOGLE_ADS_CUSTOMER_ID=<your_customer_id>
GOOGLE_ADS_USE_PROTO_PLUS=True
SHOPIFY_SHOP_URL=<your_shop_url>
SHOPIFY_ACCESS_TOKEN=<your_access_token>
SQUARE_ACCESS_TOKEN=<your_access_token>
```

## トラブルシューティング

### DuckDB接続エラー
```bash
# 破損ファイル削除
Remove-Item "./data/duckdb/commerce_test.duckdb" -Force -ErrorAction SilentlyContinue
```

### Streamlitフリーズ
```bash
# プロセス強制終了
taskkill /F /IM streamlit.exe
```

### UnicodeDecodeError
- スクリプトから絵文字を削除
- Windows環境でのPowerShellエンコーディング問題

## 成功基準
1. ✅ `python scripts/bootstrap_duckdb.py` が正常終了
2. ✅ `python scripts/run_etl.py` が正常終了
3. ✅ `python scripts/health_check.py` が正常終了
4. ✅ `streamlit run streamlit_app.py` が正常起動
5. ✅ ダッシュボードでデータが表示される
6. ✅ 統合分析レポートが自動生成される
7. ✅ データ品質監視が正常動作する

## 注意事項
- 機密情報は`.env`ファイルに保存（Gitにコミットしない）
- DuckDBファイルは`.gitignore`に追加済み
- Windows環境でのエンコーディング問題に注意
- API制限に注意（レート制限、クォータ）

## 開発環境設定
### Cursor IDE設定
- `.env`ファイルはCursorの設定で非表示になっている（cursorignore）
- 実際には存在しているため、機密情報管理に注意
- ファイルエクスプローラーで見えない場合でも、直接パス指定でアクセス可能

### 機密情報管理
- `.env`ファイル: 実際に存在（Cursorで非表示）
- `data/raw/token.pickle`: OAuth認証情報
- `data/raw/*.json`: Google API認証情報
- これらは全て`.gitignore`に追加済み

## 連絡先
プロジェクト: https://github.com/botarhythm/my-shopify-ga-app
現在のブランチ: main
最終コミット: 実データ統合完了

## 緊急時の対応
### GitHub同期ができない場合
1. **ローカルバックアップ**: 現在のコードを別フォルダにコピー
2. **新しいリポジトリ**: GitHubで新規作成
3. **機密情報なしでプッシュ**: `.env`ファイルを除外してプッシュ
4. **環境変数設定**: 新しい環境で`.env`ファイルを作成
