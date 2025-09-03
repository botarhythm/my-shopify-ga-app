# 即効パッチ実行手順

## ■ 概要
Streamlitフリーズ、mart_daily_yoy未存在、変換パイプライン未実行の3つの問題を解決する即効パッチです。

## ■ 解決内容

### 1. Streamlitフリーズ対策
- **原因**: UIスレッドで重いETL/外部コマンド実行、DuckDB同時書込衝突
- **対策**: 
  - Streamlit側は読取専用接続（`read_only=True`）
  - ETLは別プロセスで実行
  - 外部CLI呼び出しを禁止、Python APIで実行
  - `@st.cache_resource` + `@st.cache_data`で再計算を抑止

### 2. mart_daily_yoy未存在対策
- **原因**: marts作成後にYoYビルドが未実行
- **対策**: YoYをビューで定義（`CREATE OR REPLACE VIEW`）
  - 常に存在するため「存在しない」問題を根本解消
  - 自動的に最新データを反映

### 3. 変換パイプライン未実行対策
- **原因**: staging→core→marts→yoyの実行順スクリプトが未整備
- **対策**: ワンコマンドETL（`scripts/run_etl.py`）を追加
  - 初回バックフィル→日次増分→変換一式を1本化

## ■ 実行手順

### 1. ブートストラップ実行（初回のみ）
```bash
python scripts/bootstrap_duckdb.py
```

### 2. ヘルスチェック確認
```bash
python scripts/health_check.py
```

### 3. 実データ取り込み（初回は400日バックフィル→以後は増分）
```bash
python scripts/run_etl.py
```

### 4. Streamlit起動（読取専用で参照するだけに変更済み）
```bash
streamlit run streamlit_app.py
```

## ■ ファイル構成

### 新規作成ファイル
- `scripts/bootstrap.sql` - DuckDBスキーマ初期化
- `scripts/bootstrap_duckdb.py` - Pythonからブートストラップ実行
- `scripts/run_etl.py` - ETLワンコマンド化
- `scripts/health_check.py` - ヘルスチェック

### 修正ファイル
- `streamlit_app.py` - 読取専用＋キャッシュ＋軽量化

## ■ トラブルシューティング

### Streamlitが固まる場合
1. 他プロセスがDBを掴んでいないか確認（タスクマネージャーで`python.exe`の孤児プロセスを終了）
2. `scripts/run_etl.py`実行中はStreamlitを再実行しない
3. `@st.cache_data`を外していた箇所があれば戻す

### データが表示されない場合
1. ETLが正常に実行されているか確認
2. ヘルスチェックでテーブル・ビューが存在するか確認
3. 期間選択が適切か確認

### エラーが発生する場合
1. 品質チェックタブでデータ状態を確認
2. ログを確認して具体的なエラー内容を把握
3. 必要に応じてブートストラップを再実行

## ■ パフォーマンス最適化

### DuckDBチューニング
```sql
PRAGMA threads=4;                  -- CPUコア数に合わせて
PRAGMA enable_object_cache=true;   -- プラン/オブジェクトキャッシュ
PRAGMA memory_limit='3GB';         -- メモリに余裕があれば
```

### 応答性向上のポイント
- 大きいDataFrameをそのまま`st.dataframe`しない
- Plotly/Altairのマーク数を制御（日次に集約）
- 巨大CSV/Parquet直読みを禁止（DuckDBでWHERE期間＋必要列だけ）
- 接続の生存管理（`get_con_ro()`はアプリ存続中1回だけ）

## ■ 今後の改善点

1. **自動化**: cronやスケジューラーでETLを定期実行
2. **監視**: データ品質の自動監視とアラート
3. **最適化**: インデックス追加、パーティショニング
4. **拡張**: 新しいデータソースの追加

---

**作成日**: 2025-09-02  
**バージョン**: 1.0.0  
**作成者**: Cursor AI Assistant
