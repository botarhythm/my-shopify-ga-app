# 推奨コマンド集

## 基本的な開発コマンド

### 環境セットアップ
```bash
# 依存関係インストール
pip install -r config/requirements.txt

# 環境変数設定（Windows）
set SHOPIFY_API_TOKEN=your_token_here
set GOOGLE_ADS_CUSTOMER_ID=your_customer_id
set SQUARE_ACCESS_TOKEN=your_square_token
```

### アプリケーション実行
```bash
# メインダッシュボード起動
streamlit run streamlit_app.py

# 特定ポートで起動
streamlit run streamlit_app.py --server.port 8501

# ファイル監視無効で起動（安定性向上）
streamlit run streamlit_app.py --server.fileWatcherType none
```

### データ抽出・更新
```bash
# 全パイプライン実行
python src/analysis/run_analysis_pipeline.py

# 個別データ抽出
python src/extractors/shopify_data_extractor.py
python src/extractors/ga4_data_extractor.py
python src/extractors/square_data_extractor.py

# Google Ads データ生成（テスト用）
python src/ads/generate_fixtures.py --start 2025-08-01 --end 2025-08-30
```

### 開発・デバッグ
```bash
# コードフォーマット
black src/ streamlit_app.py

# リンター実行
flake8 src/ streamlit_app.py

# テスト実行
pytest src/utils/test_ga4.py

# Python環境確認
python --version
pip list
```

### Git操作
```bash
# 状態確認
git status

# 変更をコミット
git add .
git commit -m "機能追加: Google Ads統合"

# ブランチ作成・切り替え
git checkout -b feature/ads-integration
```

### Windows固有コマンド
```powershell
# ディレクトリ作成
New-Item -ItemType Directory -Path "data/ads/raw" -Force

# プロセス確認・終了
Get-Process | Where-Object {$_.ProcessName -eq "streamlit"}
Stop-Process -Name "streamlit" -Force

# 環境変数確認
Get-ChildItem Env:SHOPIFY_API_TOKEN
```

## トラブルシューティング

### Streamlit関連
```bash
# キャッシュクリア
streamlit cache clear

# 新しいポートで再起動
streamlit run streamlit_app.py --server.port 8502
```

### 依存関係問題
```bash
# 依存関係再インストール
pip uninstall -r config/requirements.txt -y
pip install -r config/requirements.txt
```