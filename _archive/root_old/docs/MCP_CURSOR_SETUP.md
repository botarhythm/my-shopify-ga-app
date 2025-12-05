# Dev3000 MCP Server Configuration for Cursor

## CursorでのMCP設定方法

### 1. 手動設定（推奨）

Cursorの設定画面で以下の手順でMCPサーバーを追加してください：

1. **Cursor Settings** → **MCP** を開く
2. **"+ New MCP Server"** をクリック
3. 以下の情報を入力：

**Server Name**: `dev3000`

**Command**: `python`

**Arguments**: 
```
src/debug/mcp_server.py --port 3684
```

**Environment Variables**:
```
PYTHONPATH=.
```

### 2. 設定ファイルを使用

`.cursor/mcp_settings.json` ファイルが作成されています。このファイルをCursorの設定ディレクトリにコピーしてください：

**Windows**:
```
%APPDATA%\Cursor\User\mcp_settings.json
```

**macOS**:
```
~/Library/Application Support/Cursor/User/mcp_settings.json
```

**Linux**:
```
~/.config/Cursor/User/mcp_settings.json
```

### 3. MCPサーバーの起動

設定後、以下のコマンドでMCPサーバーを起動してください：

```bash
python src/debug/mcp_server.py --port 3684
```

### 4. 利用可能なMCPメソッド

dev3000 MCPサーバーは以下のメソッドを提供します：

- `get_recent_logs`: 最近のログを取得
- `get_error_logs`: エラーログを取得
- `get_session_summary`: セッションサマリーを取得
- `analyze_performance`: パフォーマンス分析
- `get_ai_todos`: AI TODOを取得

### 5. Cursorでの使用例

MCPサーバーが設定されると、Cursorのチャットで以下のような質問が可能になります：

```
"最新のエラーを分析して解決策を提案してください"
"パフォーマンスの問題を特定してください"
"現在のセッションの状況を教えてください"
```

### 6. トラブルシューティング

**MCPサーバーが起動しない場合**:
- Pythonのパスが正しいか確認
- `src/debug/mcp_server.py` ファイルが存在するか確認
- ポート3684が使用可能か確認

**CursorでMCPが認識されない場合**:
- Cursorを再起動
- MCP設定を再確認
- ログファイルでエラーを確認

### 7. ログの確認

MCPサーバーのログは `logs/` ディレクトリに保存されます：

```bash
# ログファイルの確認
ls -la logs/

# 最新のログを確認
tail -f logs/dev3000_session_*.jsonl
```

