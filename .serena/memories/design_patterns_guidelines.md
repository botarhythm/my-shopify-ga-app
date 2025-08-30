# 設計パターン・ガイドライン

## アーキテクチャパターン

### 1. データフロー設計
```
データソース → 抽出 → 処理 → 表示
    ↓           ↓      ↓      ↓
  API/CSV → extractors/ → analysis/ → streamlit_app.py
```

### 2. 責任分離
- **extractors/**: データ取得のみ
- **analysis/**: データ処理・分析のみ
- **ui/**: 表示・インタラクション
- **utils/**: 共通機能

## コーディングパターン

### 3. エラーハンドリングパターン
```python
def safe_data_operation(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"安全なデータ操作\"\"\"
    try:
        if df.empty:
            return pd.DataFrame()
        
        # データ処理
        result = process_data(df)
        return result
    except Exception as e:
        st.error(f\"データ処理エラー: {e}\")
        return pd.DataFrame()
```

### 4. 設定管理パターン
```python
# 環境変数 + YAMLファイル
config = yaml.safe_load(open('config/google_ads.yaml'))
api_token = os.getenv('SHOPIFY_API_TOKEN')
```

### 5. データバリデーションパターン
```python
from pydantic import BaseModel, validator

class CampaignRow(BaseModel):
    campaign_id: str
    cost: float
    
    @validator('cost')
    def cost_must_be_positive(cls, v):
        if v < 0:
            raise ValueError('Cost must be positive')
        return v
```

## Streamlit UIパターン

### 6. KPIカード表示パターン
```python
col1, col2, col3 = st.columns(3)

with col1:
    delta_value = current - previous
    if delta_value > 0:
        st.success(f\"📈 **指標名**\\n{current:,}\\n+{delta_value:,} (+{percentage:.1f}%)\")
    else:
        st.error(f\"📉 **指標名**\\n{current:,}\\n{delta_value:,} ({percentage:.1f}%)\")
```

### 7. データ読み込みパターン
```python
@st.cache_data(show_spinner=False)
def load_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.error(f\"CSV読み込みエラー: {e}\")
        return pd.DataFrame()
```

### 8. チャート作成パターン
```python
fig = px.bar(
    data,
    x='category',
    y='value',
    title='タイトル',
    labels={'value': '値', 'category': 'カテゴリ'},
    color='value',
    color_continuous_scale='Blues'
)
fig.update_layout(height=400, showlegend=False)
st.plotly_chart(fig, use_container_width=True)
```

## API統合パターン

### 9. 認証管理パターン
```python
class APIClientFactory:
    @classmethod
    def create_client(cls):
        credentials = cls._load_credentials()
        return cls._build_client(credentials)
    
    @classmethod
    def _load_credentials(cls):
        # 環境変数から認証情報を取得
        pass
```

### 10. リトライパターン
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def api_call():
    # API呼び出し
    pass
```

## データ処理パターン

### 11. 数値変換パターン
```python
df['numeric_column'] = pd.to_numeric(df['string_column'], errors='coerce').fillna(0)
```

### 12. 日付処理パターン
```python
df['date'] = pd.to_datetime(df['date_string'])
df['date_only'] = df['created_at'].dt.date
```

### 13. 集計パターン
```python
summary = df.groupby('category').agg({
    'sales': 'sum',
    'sessions': 'sum',
    'conversion_rate': 'mean'
}).reset_index()
```

## パフォーマンス最適化

### 14. キャッシュ活用
```python
@st.cache_data(ttl=300)  # 5分間キャッシュ
def expensive_computation(data):
    return process_large_dataset(data)
```

### 15. 遅延読み込み
```python
if st.button(\"詳細分析を実行\"):
    with st.spinner(\"分析中...\"):
        result = heavy_analysis()
        st.success(\"分析完了\")
```