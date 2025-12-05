"""
修正されたKPIダッシュボードタブ
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """DuckDB接続を取得（読取専用）"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
    return con

def render_revenue_breakdown(start_date: str, end_date: str):
    """売上内訳を表示（完全に作り直し）"""
    st.subheader("💰 売上内訳")
    
    # 直接データベースから最新データを取得
    con = get_db_connection()
    try:
        print(f"DEBUG: 売上内訳計算開始 - {start_date} から {end_date}")
        
        # 1. Shopify売上（正しい計算方法を使用）
        shopify_revenue = 0
        try:
            # まずテーブル構造を確認
            columns_result = con.execute("DESCRIBE core_shopify").fetchall()
            available_columns = [col[0] for col in columns_result]
            print(f"DEBUG: core_shopify 利用可能な列: {available_columns}")
            
            # price * qty で売上を計算（正しい方法）
            if 'price' in available_columns and 'qty' in available_columns and 'date' in available_columns:
                shopify_query = """
                SELECT SUM(price * qty) as shopify_revenue
                FROM core_shopify
                WHERE date BETWEEN ? AND ?
                """
                shopify_result = con.execute(shopify_query, [start_date, end_date]).fetchone()
                shopify_revenue = shopify_result[0] if shopify_result[0] is not None else 0
                print(f"DEBUG: Shopify売上（price * qty）: ¥{shopify_revenue:,.0f}")
            else:
                print("DEBUG: Shopify売上計算に必要な列が見つかりません")
                
        except Exception as e:
            print(f"DEBUG: Shopify売上取得エラー: {e}")
            shopify_revenue = 0
        
        # 2. Square売上（正しい分離計算）
        square_coffee_revenue = 0
        square_invoice_revenue = 0
        
        try:
            # Squareテーブル構造を確認
            columns_result = con.execute("DESCRIBE core_square").fetchall()
            available_columns = [col[0] for col in columns_result]
            print(f"DEBUG: core_square 利用可能な列: {available_columns}")
            
            if 'amount' in available_columns and 'date' in available_columns and 'payment_id' in available_columns:
                # Squareコーヒー売上（請求書以外）
                square_coffee_query = """
                SELECT SUM(amount) as square_coffee_revenue
                FROM core_square
                WHERE date BETWEEN ? AND ?
                AND payment_id != '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
                """
                square_coffee_result = con.execute(square_coffee_query, [start_date, end_date]).fetchone()
                square_coffee_revenue = square_coffee_result[0] if square_coffee_result[0] is not None else 0
                print(f"DEBUG: Squareコーヒー売上: ¥{square_coffee_revenue:,.0f}")
                
                # Square請求書売上
                square_invoice_query = """
                SELECT SUM(amount) as square_invoice_revenue
                FROM core_square
                WHERE date BETWEEN ? AND ?
                AND payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
                """
                square_invoice_result = con.execute(square_invoice_query, [start_date, end_date]).fetchone()
                square_invoice_revenue = square_invoice_result[0] if square_invoice_result[0] is not None else 0
                print(f"DEBUG: Square請求書売上: ¥{square_invoice_revenue:,.0f}")
                
                # 請求書データの詳細確認
                debug_invoice_query = """
                SELECT payment_id, amount, date, status
                FROM core_square
                WHERE payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
                """
                debug_result = con.execute(debug_invoice_query).fetchall()
                if debug_result:
                    print(f"DEBUG: 請求書データ詳細: {debug_result}")
                else:
                    print("DEBUG: 請求書データが見つかりません")
                    
            else:
                print("DEBUG: Square売上計算に必要な列が見つかりません")
                
        except Exception as e:
            print(f"DEBUG: Square売上取得エラー: {e}")
            square_coffee_revenue = 0
            square_invoice_revenue = 0
        
        # 3. 総売上計算
        total_revenue = shopify_revenue + square_coffee_revenue + square_invoice_revenue
        print(f"DEBUG: 総売上: ¥{total_revenue:,.0f}")
        
        # 4. デバッグ情報をダッシュボードに表示
        st.write("**🔍 デバッグ情報:**")
        st.write(f"- Shopify売上: ¥{shopify_revenue:,.0f}")
        st.write(f"- Squareコーヒー売上: ¥{square_coffee_revenue:,.0f}")
        st.write(f"- Square請求書売上: ¥{square_invoice_revenue:,.0f}")
        st.write(f"- 総売上: ¥{total_revenue:,.0f}")
        
    finally:
        con.close()
    
    # 5. メトリックカードの表示
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        shopify_ratio = (shopify_revenue / total_revenue * 100) if total_revenue > 0 else 0
        st.metric(
            label="Shopify売上",
            value=f"¥{shopify_revenue:,.0f}",
            delta=None
        )
        st.caption(f"構成比: {shopify_ratio:.1f}%")
    
    with col2:
        square_coffee_ratio = (square_coffee_revenue / total_revenue * 100) if total_revenue > 0 else 0
        st.metric(
            label="Squareコーヒー売上",
            value=f"¥{square_coffee_revenue:,.0f}",
            delta=None
        )
        st.caption(f"構成比: {square_coffee_ratio:.1f}%")
    
    with col3:
        square_invoice_ratio = (square_invoice_revenue / total_revenue * 100) if total_revenue > 0 else 0
        st.metric(
            label="Square請求書売上",
            value=f"¥{square_invoice_revenue:,.0f}",
            delta=None
        )
        st.caption(f"構成比: {square_invoice_ratio:.1f}%")
    
    with col4:
        st.metric(
            label="総売上",
            value=f"¥{total_revenue:,.0f}"
        )
    
    # 6. データがない場合の警告
    if total_revenue == 0:
        st.info("📊 指定された期間の売上データがありません")
        return
    
    if square_coffee_revenue == 0 and square_invoice_revenue == 0:
        st.warning("⚠️ Squareの売上データは現在取得できていません。API認証またはデータ取得に問題がある可能性があります。")
    
    # 7. 円グラフの表示
    if total_revenue > 0:
        labels = []
        values = []
        colors = []
        
        if shopify_revenue > 0:
            labels.append('Shopify')
            values.append(shopify_revenue)
            colors.append('#1f77b4')
        
        if square_coffee_revenue > 0:
            labels.append('Squareコーヒー')
            values.append(square_coffee_revenue)
            colors.append('#ff7f0e')
        
        if square_invoice_revenue > 0:
            labels.append('Square請求書')
            values.append(square_invoice_revenue)
            colors.append('#2ca02c')
        
        if labels:  # データがある場合のみグラフを表示
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=0.3,
                marker_colors=colors
            )])
            
            fig.update_layout(
                title="売上内訳",
                height=400,
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)

def render_traffic_metrics(df: pd.DataFrame):
    """トラフィック指標を表示（修正版）"""
    if df.empty:
        st.info("📊 指定された期間のデータがありません")
        return
    
    # 利用可能な列を確認
    available_columns = df.columns.tolist()
    print(f"DEBUG: render_traffic_metrics 利用可能な列: {available_columns}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # セッション数
        if "sessions" in available_columns:
            fig = px.line(
                df, x="date", y="sessions",
                title="セッション数推移",
                labels={"sessions": "セッション数", "date": "日付"}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("セッション数データが利用できません")
    
    with col2:
        # 購入数
        if "purchases" in available_columns:
            fig = px.line(
                df, x="date", y="purchases",
                title="購入数推移",
                labels={"purchases": "購入数", "date": "日付"}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("購入数データが利用できません")
    
    # AI分析結果を追加
    st.subheader("🤖 AI分析結果 - トラフィック指標")
    
    # AI分析結果の表示
    analysis_col1, analysis_col2 = st.columns(2)
    
    with analysis_col1:
        if "sessions" in available_columns:
            total_sessions = float(df["sessions"].sum())
            avg_sessions = float(df["sessions"].mean())
            max_sessions = float(df["sessions"].max())
            min_sessions = float(df["sessions"].min())
            
            st.markdown(f"""
            **📈 セッション分析**
            - 総セッション数: {total_sessions:,}回
            - 平均日次セッション: {avg_sessions:.1f}回
            - 最高セッション: {max_sessions:,}回
            - 最低セッション: {min_sessions:,}回
            """)
        else:
            st.info("セッション分析データが利用できません")
    
    with analysis_col2:
        if "purchases" in available_columns:
            total_purchases = float(df["purchases"].sum())
            avg_purchases = float(df["purchases"].mean())
            
            # コンバージョン率の計算
            if "sessions" in available_columns:
                total_sessions = float(df["sessions"].sum())
                overall_cvr = (total_purchases / total_sessions) * 100 if total_sessions > 0 else 0
            else:
                overall_cvr = 0
            
            st.markdown(f"""
            **🛒 購入分析**
            - 総購入数: {total_purchases:,}回
            - 平均日次購入: {avg_purchases:.1f}回
            - 全体CVR: {overall_cvr:.2f}%
            """)
        else:
            st.info("購入分析データが利用できません")

# テスト用のメイン関数
if __name__ == "__main__":
    st.title("修正されたKPIダッシュボード")
    render_revenue_breakdown("2025-08-01", "2025-08-31")

