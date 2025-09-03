# タスク完了時のチェックリスト

## コード品質チェック

### 1. フォーマット・リンター
```bash
# コードフォーマット実行
black src/ streamlit_app.py

# リンター実行（エラー0を目指す）
flake8 src/ streamlit_app.py --max-line-length=120
```

### 2. 型チェック
- 関数の型ヒントが適切に設定されているか
- Pydanticスキーマが正しく定義されているか
- Union型は `str | None` 形式を使用しているか

### 3. エラーハンドリング
- try-except文で適切な例外処理
- ユーザー向けエラーメッセージは日本語
- デフォルト値・フォールバック処理の実装

## 機能テスト

### 4. データ抽出テスト
```bash
# 各抽出スクリプトの動作確認
python src/extractors/shopify_data_extractor.py
python src/extractors/ga4_data_extractor.py
python src/extractors/square_data_extractor.py
```

### 5. ダッシュボードテスト
```bash
# Streamlitアプリの起動確認
streamlit run streamlit_app.py --server.port 8501
```
- 全てのKPIが正しく表示されるか
- チャート・グラフが適切に描画されるか
- フィルター機能が動作するか
- エラーメッセージが適切に表示されるか

### 6. Google Ads統合テスト（該当する場合）
```bash
# フィクスチャデータ生成
python src/ads/generate_fixtures.py --start 2025-08-01 --end 2025-08-30

# 認証テスト
python src/ads/google_ads_client.py
```

## ドキュメント更新

### 7. README・ドキュメント
- 新機能の説明を追加
- 設定手順の更新
- 使用方法の説明

### 8. 設定ファイル
- `config/requirements.txt`の依存関係更新
- `config/google_ads.yaml`の設定確認
- `.env.example`の環境変数例更新

## デプロイ準備

### 9. 環境変数確認
```bash
# 必要な環境変数が設定されているか
echo $SHOPIFY_API_TOKEN
echo $GOOGLE_ADS_CUSTOMER_ID
echo $SQUARE_ACCESS_TOKEN
```

### 10. ファイル整理
- 不要なファイル・コメントの削除
- デバッグ用コードの削除
- ログファイルのクリーンアップ

## 最終確認

### 11. パフォーマンステスト
- 大量データでの動作確認
- メモリ使用量の確認
- レスポンス時間の測定

### 12. ユーザビリティテスト
- UI/UXの直感性
- エラー時の分かりやすさ
- 日本語表示の自然さ

## Git管理

### 13. コミット・プッシュ
```bash
# 変更をステージング
git add .

# 意味のあるコミットメッセージ
git commit -m "feat: Google Ads統合機能を追加"

# リモートにプッシュ
git push origin main
```