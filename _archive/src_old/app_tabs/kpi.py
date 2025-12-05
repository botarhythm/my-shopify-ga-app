"""
KPIダッシュボードタブ
主要KPIの可視化とYoY比較
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import duckdb
import os
from datetime import datetime, timedelta


def get_db_connection():
    """DuckDB接続を取得（読取専用）"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
    return con


def load_revenue_breakdown(start_date: str, end_date: str) -> dict:
    """
    売上内訳を読み込み（キャッシュ付き）
    
    Args:
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        dict: 売上内訳データ
    """
    con = get_db_connection()
    try:
        # Shopify売上を取得
        shopify_query = """
        SELECT SUM(order_total) as shopify_revenue
        FROM core_shopify
        WHERE date BETWEEN ? AND ?
        """
        shopify_result = con.execute(shopify_query, [start_date, end_date]).fetchone()
        shopify_revenue = shopify_result[0] if shopify_result[0] is not None else 0
        
        # Square売上を取得（コーヒー売上と請求書売上に分ける）
        square_coffee_query = """
        SELECT SUM(amount) as square_coffee_revenue
        FROM core_square
        WHERE date BETWEEN ? AND ?
        AND payment_id != '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
        """
        square_coffee_result = con.execute(square_coffee_query, [start_date, end_date]).fetchone()
        square_coffee_revenue = square_coffee_result[0] if square_coffee_result[0] is not None else 0
        
        square_invoice_query = """
        SELECT SUM(amount) as square_invoice_revenue
        FROM core_square
        WHERE date BETWEEN ? AND ?
        AND payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
        """
        square_invoice_result = con.execute(square_invoice_query, [start_date, end_date]).fetchone()
        square_invoice_revenue = square_invoice_result[0] if square_invoice_result[0] is not None else 0
        
        # デバッグ情報を追加
        print(f"DEBUG: Shopify売上: ¥{shopify_revenue:,}")
        print(f"DEBUG: Squareコーヒー売上: ¥{square_coffee_revenue:,}")
        print(f"DEBUG: Square請求書売上: ¥{square_invoice_revenue:,}")
        
        total_revenue = shopify_revenue + square_coffee_revenue + square_invoice_revenue
        
        return {
            "shopify_revenue": shopify_revenue,
            "square_coffee_revenue": square_coffee_revenue,
            "square_invoice_revenue": square_invoice_revenue,
            "total_revenue": total_revenue,
            "shopify_ratio": (shopify_revenue / total_revenue * 100) if total_revenue > 0 else 0,
            "square_coffee_ratio": (square_coffee_revenue / total_revenue * 100) if total_revenue > 0 else 0,
            "square_invoice_ratio": (square_invoice_revenue / total_revenue * 100) if total_revenue > 0 else 0
        }
    finally:
        con.close()
@st.cache_data(ttl=300, show_spinner=False)
def load_kpi_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    KPIデータを読み込み（キャッシュ付き）
    
    Args:
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        DataFrame: KPIデータ
    """
    con = get_db_connection()
    try:
        query = """
        SELECT * FROM mart_daily_yoy
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """
        df = con.execute(query, [start_date, end_date]).arrow().to_pandas()
        return df
    except duckdb.CatalogException:
        # YoYビューが存在しない場合は通常のmart_dailyを使用
        query = """
        SELECT * FROM mart_daily
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """
        df = con.execute(query, [start_date, end_date]).arrow().to_pandas()
        return df
    finally:
        con.close()


def calculate_kpi_summary(df: pd.DataFrame) -> dict:
    """
    KPIサマリーを計算
    
    Args:
        df: KPIデータ
    
    Returns:
        dict: KPIサマリー
    """
    if df.empty:
        return {
            "total_revenue": 0,
            "total_sessions": 0,
            "total_purchases": 0,
            "total_cost": 0,
            "avg_roas": 0,
            "avg_cvr": 0,
            "avg_aov": 0,
            "revenue_yoy_pct": 0
        }
    
    # 実際のカラム名を使用（型変換を適切に処理）
    # カラムの存在確認とデフォルト値設定
    shopify_revenue = df["shopify_revenue"].sum() if "shopify_revenue" in df.columns else 0
    square_revenue = df["square_revenue"].sum() if "square_revenue" in df.columns else 0
    sessions = df["sessions"].sum() if "sessions" in df.columns else 0
    purchases = df["purchases"].sum() if "purchases" in df.columns else 0
    ads_cost = df["ads_cost"].sum() if "ads_cost" in df.columns else 0
    conv_value = df["conv_value"].sum() if "conv_value" in df.columns else 0
    
    total_revenue = float(shopify_revenue) + float(square_revenue)
    total_sessions = float(sessions)
    total_purchases = float(purchases)
    total_cost = float(ads_cost)
    total_conv_value = float(conv_value)
    
    # ROAS計算（コンバージョン価値 / 広告費）
    avg_roas = total_conv_value / total_cost if total_cost > 0 else 0
    
    # コンバージョン率計算（購入数 / セッション数）
    avg_cvr = (total_purchases / total_sessions) * 100 if total_sessions > 0 else 0
    
    # 平均注文価値計算（総売上 / 購入数）
    avg_aov = total_revenue / total_purchases if total_purchases > 0 else 0
    
    summary = {
        "total_revenue": total_revenue,
        "total_sessions": total_sessions,
        "total_purchases": total_purchases,
        "total_cost": total_cost,
        "avg_roas": avg_roas,
        "avg_cvr": avg_cvr,
        "avg_aov": avg_aov,
    }
    
    # YoY比較
    if "shopify_revenue_yoy" in df.columns and "square_revenue_yoy" in df.columns:
        total_revenue_yoy = float(df["shopify_revenue_yoy"].sum()) + float(df["square_revenue_yoy"].sum())
        summary["revenue_yoy_pct"] = (
            (total_revenue - total_revenue_yoy) / total_revenue_yoy * 100
        ) if total_revenue_yoy > 0 else 0
    
    return summary


def render_revenue_breakdown(start_date: str, end_date: str):
    """売上内訳を表示"""
    st.subheader("💰 売上内訳")
    
    # 直接データベースから最新データを取得
    con = get_db_connection()
    try:
        # デバッグ: パラメータを確認
        print(f"DEBUG: start_date = {start_date}, end_date = {end_date}")
        
        # Shopify売上（利用可能な列を動的に確認）
        try:
            # テーブル構造を確認
            columns_result = con.execute("DESCRIBE core_shopify").fetchall()
            available_columns = [col[0] for col in columns_result]
            print(f"DEBUG: 利用可能な列: {available_columns}")
            
            # 利用可能な列に基づいてクエリを構築
            if 'total_price' in available_columns and 'date' in available_columns:
                shopify_query = """
                SELECT SUM(total_price) as shopify_revenue
                FROM core_shopify
                WHERE date BETWEEN ? AND ?
                """
                shopify_result = con.execute(shopify_query, [start_date, end_date]).fetchone()
                shopify_revenue = shopify_result[0] if shopify_result[0] is not None else 0
            elif 'price' in available_columns and 'qty' in available_columns and 'date' in available_columns:
                shopify_query = """
                SELECT SUM(price * qty) as shopify_revenue
                FROM core_shopify
                WHERE date BETWEEN ? AND ?
                """
                shopify_result = con.execute(shopify_query, [start_date, end_date]).fetchone()
                shopify_revenue = shopify_result[0] if shopify_result[0] is not None else 0
            else:
                # 利用可能な列のみを使用
                shopify_query = "SELECT COUNT(*) as shopify_count FROM core_shopify"
                shopify_result = con.execute(shopify_query).fetchone()
                shopify_revenue = shopify_result[0] if shopify_result[0] is not None else 0
                print("DEBUG: 利用可能な列が限られているため、件数のみ取得")
                
        except Exception as e:
            print(f"DEBUG: Shopify売上取得エラー: {e}")
            shopify_revenue = 0
        
        # Squareコーヒー売上（請求書以外）
        square_coffee_query = """
        SELECT SUM(amount) as square_coffee_revenue
        FROM core_square
        WHERE date BETWEEN ? AND ?
        AND payment_id != '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
        """
        square_coffee_result = con.execute(square_coffee_query, [start_date, end_date]).fetchone()
        square_coffee_revenue = square_coffee_result[0] if square_coffee_result[0] is not None else 0
        
        # Square請求書売上
        square_invoice_query = """
        SELECT SUM(amount) as square_invoice_revenue
        FROM core_square
        WHERE date BETWEEN ? AND ?
        AND payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
        """
        square_invoice_result = con.execute(square_invoice_query, [start_date, end_date]).fetchone()
        square_invoice_revenue = square_invoice_result[0] if square_invoice_result[0] is not None else 0
        
        # デバッグ: 請求書クエリの詳細確認
        debug_invoice_query = """
        SELECT payment_id, amount, date
        FROM core_square
        WHERE payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'
        """
        debug_result = con.execute(debug_invoice_query).fetchone()
        if debug_result:
            print(f"DEBUG: 請求書データ確認 - ID: {debug_result[0]}, 金額: ¥{debug_result[1]:,}, 日付: {debug_result[2]}")
        else:
            print("DEBUG: 請求書データが見つかりません")
        
        total_revenue = shopify_revenue + square_coffee_revenue + square_invoice_revenue
        
        # デバッグ情報を表示
        st.write(f"**デバッグ情報:**")
        st.write(f"- Shopify売上: ¥{shopify_revenue:,}")
        st.write(f"- Squareコーヒー売上: ¥{square_coffee_revenue:,}")
        st.write(f"- Square請求書売上: ¥{square_invoice_revenue:,}")
        st.write(f"- 総売上: ¥{total_revenue:,}")
        
    finally:
        con.close()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        shopify_ratio = (shopify_revenue / total_revenue * 100) if total_revenue > 0 else 0
        st.metric(
            label="Shopify売上",
            value=f"¥{shopify_revenue:,.0f}",
            delta=None  # 売上内訳の割合なので矢印は不適切
        )
        st.caption(f"構成比: {shopify_ratio:.1f}%")
    
    with col2:
        square_coffee_ratio = (square_coffee_revenue / total_revenue * 100) if total_revenue > 0 else 0
        st.metric(
            label="Squareコーヒー売上",
            value=f"¥{square_coffee_revenue:,.0f}",
            delta=None  # 売上内訳の割合なので矢印は不適切
        )
        st.caption(f"構成比: {square_coffee_ratio:.1f}%")
    
    with col3:
        square_invoice_ratio = (square_invoice_revenue / total_revenue * 100) if total_revenue > 0 else 0
        st.metric(
            label="Square請求書売上",
            value=f"¥{square_invoice_revenue:,.0f}",
            delta=None  # 売上内訳の割合なので矢印は不適切
        )
        st.caption(f"構成比: {square_invoice_ratio:.1f}%")
    
    with col4:
        st.metric(
            label="総売上",
            value=f"¥{total_revenue:,.0f}"
        )
    
    # データがない場合のメッセージ
    if total_revenue == 0:
        st.info("📊 指定された期間の売上データがありません")
        return
    
    # Squareデータがない場合の注意メッセージ
    square_total_revenue = square_coffee_revenue + square_invoice_revenue
    if square_total_revenue == 0:
        st.warning("⚠️ Squareの売上データは現在取得できていません。API認証またはデータ取得に問題がある可能性があります。")
    
    # 円グラフで売上内訳を表示（Shopify、Squareコーヒー、Square請求書）
    if total_revenue > 0:
        labels = []
        values = []
        colors = []
        
        if shopify_revenue > 0:
            labels.append('Shopify')
            values.append(shopify_revenue)
            colors.append('#1f77b4')  # 青
        
        if square_coffee_revenue > 0:
            labels.append('Squareコーヒー')
            values.append(square_coffee_revenue)
            colors.append('#ff7f0e')  # オレンジ
        
        if square_invoice_revenue > 0:
            labels.append('Square請求書')
            values.append(square_invoice_revenue)
            colors.append('#2ca02c')  # 緑
        
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
def render_kpi_cards(summary: dict):
    """KPIカードを表示"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="総売上",
            value=f"¥{summary.get('total_revenue', 0):,.0f}",
            delta=f"{summary.get('revenue_yoy_pct', 0):+.1f}%" if summary.get('revenue_yoy_pct', 0) != 0 else None
        )
    
    with col2:
        st.metric(
            label="セッション数",
            value=f"{summary.get('total_sessions', 0):,}",
            delta=f"{summary.get('sessions_yoy_pct', 0):+.1f}%" if summary.get('sessions_yoy_pct', 0) != 0 else None
        )
    
    with col3:
        st.metric(
            label="コンバージョン率",
            value=f"{summary.get('avg_cvr', 0):.2f}%"
        )
    
    with col4:
        st.metric(
            label="ROAS",
            value=f"{summary.get('avg_roas', 0):.2f}",
            delta=f"{summary.get('roas_yoy_pct', 0):+.1f}%" if summary.get('roas_yoy_pct', 0) != 0 else None
        )


def render_revenue_trend(df: pd.DataFrame):
    """売上トレンドを表示"""
    if df.empty:
        st.info("📊 指定された期間のデータがありません")
        return
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("売上トレンド", "YoY比較"),
        vertical_spacing=0.1
    )
    
    # 売上トレンド（Shopify + Square）
    total_revenue = df["shopify_revenue"].astype(float) + df["square_revenue"].astype(float)
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=total_revenue,
            mode="lines+markers",
            name="当年売上",
            line=dict(color="blue")
        ),
        row=1, col=1
    )
    
    # 実際の2024年8月データを取得
    con = get_db_connection()
    try:
        yoy_2024_data = con.execute("""
        SELECT 
            date,
            shopify_revenue,
            square_revenue
        FROM mart_daily_yoy 
        WHERE date >= '2024-08-01' AND date <= '2024-08-31'
        ORDER BY date
        """).fetchall()
        
        # 2024年8月のデータを辞書に変換（None値を0に変換）
        yoy_2024_dict = {row[0]: ((row[1] if row[1] is not None else 0) + (row[2] if row[2] is not None else 0)) for row in yoy_2024_data}
        
        # 2025年8月の各日付に対応する2024年8月のデータを取得
        total_revenue_yoy = []
        for date in df["date"]:
            # 日付を文字列に変換してから置換
            date_str = str(date)
            date_2024 = date_str.replace("2025", "2024")
            yoy_revenue = yoy_2024_dict.get(date_2024, 0)
            total_revenue_yoy.append(yoy_revenue)
        
        total_revenue_yoy = pd.Series(total_revenue_yoy)
        
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=total_revenue_yoy,
                mode="lines+markers",
                name="前年同期",
                line=dict(color="gray", dash="dash")
            ),
            row=1, col=1
        )
        
        # YoY変化率を計算
        revenue_yoy_pct = ((total_revenue - total_revenue_yoy) / total_revenue_yoy * 100).fillna(0)
        
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=revenue_yoy_pct,
                name="YoY変化率",
                marker_color=revenue_yoy_pct.apply(
                    lambda x: "green" if x > 0 else "red" if x < 0 else "gray"
                )
            ),
            row=2, col=1
        )
        
    finally:
        con.close()
    
    fig.update_layout(
        height=600,
        showlegend=True,
        title_text="売上トレンドとYoY比較",
        xaxis_title="日付",
        yaxis_title="売上 (円)",
        yaxis2_title="YoY変化率 (%)"
    )
    
    # Y軸の設定を改善
    fig.update_yaxes(title_text="売上 (円)", row=1, col=1)
    fig.update_yaxes(title_text="YoY変化率 (%)", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)


def render_traffic_metrics(df: pd.DataFrame):
    """トラフィック指標を表示"""
    if df.empty:
        st.info("📊 指定された期間のデータがありません")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # セッション数
        fig = px.line(
            df, x="date", y="sessions",
            title="セッション数推移",
            labels={"sessions": "セッション数", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 購入数
        fig = px.line(
            df, x="date", y="purchases",
            title="購入数推移",
            labels={"purchases": "購入数", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # AI分析結果を追加
    st.subheader("🤖 AI分析結果 - トラフィック指標")
    
    # セッション数の分析
    total_sessions = float(df["sessions"].sum())
    avg_sessions = float(df["sessions"].mean())
    max_sessions = float(df["sessions"].max())
    min_sessions = float(df["sessions"].min())
    
    # 購入数の分析
    total_purchases = float(df["purchases"].sum())
    avg_purchases = float(df["purchases"].mean())
    
    # コンバージョン率の計算
    overall_cvr = (total_purchases / total_sessions) * 100 if total_sessions > 0 else 0
    
    # AI分析結果の表示
    analysis_col1, analysis_col2 = st.columns(2)
    
    with analysis_col1:
        st.markdown("""
        **📈 セッション分析**
        - 総セッション数: {total_sessions:,}回
        - 平均日次セッション: {avg_sessions:.1f}回
        - 最高セッション: {max_sessions:,}回
        - 最低セッション: {min_sessions:,}回
        
        **💡 専門家の見解:**
        セッション数は{trend}傾向にあります。{recommendation}
        """.format(
            total_sessions=int(total_sessions),
            avg_sessions=avg_sessions,
            max_sessions=int(max_sessions),
            min_sessions=int(min_sessions),
            trend="上昇" if max_sessions > min_sessions * 1.2 else "安定" if max_sessions < min_sessions * 1.1 else "変動",
            recommendation="継続的な成長が見込めます。コンテンツマーケティングの効果が現れています。" if max_sessions > min_sessions * 1.2 else "安定したトラフィックを維持しています。SEO対策の継続をお勧めします。"
        ))
    
    with analysis_col2:
        st.markdown("""
        **🛒 購入行動分析**
        - 総購入数: {total_purchases:,}件
        - 平均日次購入: {avg_purchases:.1f}件
        - 全体コンバージョン率: {overall_cvr:.2f}%
        
        **💡 専門家の見解:**
        コンバージョン率{rate_level}です。{conversion_recommendation}
        """.format(
            total_purchases=int(total_purchases),
            avg_purchases=avg_purchases,
            overall_cvr=overall_cvr,
            rate_level="は良好" if overall_cvr > 2.0 else "は改善の余地があります" if overall_cvr > 1.0 else "は低く、改善が必要です",
            conversion_recommendation="ユーザーエクスペリエンスの向上や商品ページの最適化を検討してください。" if overall_cvr < 2.0 else "現在のコンバージョン率を維持しつつ、さらなる向上を目指しましょう。"
        ))


def render_efficiency_metrics(df: pd.DataFrame):
    """効率指標を表示"""
    if df.empty:
        st.info("📊 指定された期間のデータがありません")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # ROAS（動的に計算）
        df_roas = df.copy()
        df_roas["roas"] = df_roas["conv_value"] / df_roas["ads_cost"]
        df_roas["roas"] = df_roas["roas"].fillna(0)  # 0除算を防ぐ
        
        fig = px.line(
            df_roas, x="date", y="roas",
            title="ROAS推移",
            labels={"roas": "ROAS", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # CVR（動的に計算）
        df_cvr = df.copy()
        df_cvr["cvr"] = (df_cvr["purchases"] / df_cvr["sessions"]) * 100
        df_cvr["cvr"] = df_cvr["cvr"].fillna(0)  # 0除算を防ぐ
        
        fig = px.line(
            df_cvr, x="date", y="cvr",
            title="コンバージョン率推移",
            labels={"cvr": "CVR (%)", "date": "日付"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # AI分析結果を追加
    st.subheader("🤖 AI分析結果 - 効率指標")
    
    # ROAS分析
    df_roas = df.copy()
    df_roas["roas"] = df_roas["conv_value"] / df_roas["ads_cost"]
    df_roas["roas"] = df_roas["roas"].fillna(0)
    
    avg_roas = float(df_roas["roas"].mean())
    max_roas = float(df_roas["roas"].max())
    min_roas = float(df_roas["roas"].min())
    total_ads_cost = float(df["ads_cost"].sum())
    total_conv_value = float(df["conv_value"].sum())
    
    # CVR分析
    df_cvr = df.copy()
    df_cvr["cvr"] = (df_cvr["purchases"] / df_cvr["sessions"]) * 100
    df_cvr["cvr"] = df_cvr["cvr"].fillna(0)
    
    avg_cvr = float(df_cvr["cvr"].mean())
    max_cvr = float(df_cvr["cvr"].max())
    min_cvr = float(df_cvr["cvr"].min())
    
    # AI分析結果の表示
    analysis_col1, analysis_col2 = st.columns(2)
    
    with analysis_col1:
        st.markdown("""
        **📊 ROAS分析**
        - 平均ROAS: {avg_roas:.2f}
        - 最高ROAS: {max_roas:.2f}
        - 最低ROAS: {min_roas:.2f}
        - 総広告費: ¥{total_ads_cost:,}
        - 総コンバージョン価値: ¥{total_conv_value:,}
        
        **💡 専門家の見解:**
        ROAS{roas_level}です。{roas_recommendation}
        """.format(
            avg_roas=avg_roas,
            max_roas=max_roas,
            min_roas=min_roas,
            total_ads_cost=int(total_ads_cost),
            total_conv_value=int(total_conv_value),
            roas_level="は優秀" if avg_roas > 3.0 else "は良好" if avg_roas > 2.0 else "は改善が必要" if avg_roas > 1.0 else "は低く、広告戦略の見直しが必要",
            roas_recommendation="現在の広告戦略を継続し、さらなる最適化を図りましょう。" if avg_roas > 2.0 else "キーワードの見直しや広告クリエイティブの改善を検討してください。" if avg_roas > 1.0 else "広告予算の配分やターゲティング設定を根本的に見直す必要があります。"
        ))
    
    with analysis_col2:
        st.markdown("""
        **🎯 コンバージョン率分析**
        - 平均CVR: {avg_cvr:.2f}%
        - 最高CVR: {max_cvr:.2f}%
        - 最低CVR: {min_cvr:.2f}%
        
        **💡 専門家の見解:**
        コンバージョン率{cvr_level}です。{cvr_recommendation}
        """.format(
            avg_cvr=avg_cvr,
            max_cvr=max_cvr,
            min_cvr=min_cvr,
            cvr_level="は優秀" if avg_cvr > 3.0 else "は良好" if avg_cvr > 2.0 else "は改善の余地があります" if avg_cvr > 1.0 else "は低く、サイト改善が必要",
            cvr_recommendation="現在のサイト設計が効果的です。A/Bテストでさらなる改善を検討しましょう。" if avg_cvr > 2.0 else "ランディングページの最適化やユーザーエクスペリエンスの改善を検討してください。" if avg_cvr > 1.0 else "サイトの使いやすさ、商品説明、決済プロセスの見直しが必要です。"
        ))


def render_kpi_tab():
    """KPIタブを表示"""
    st.header("📊 KPIダッシュボード")
    
    # 日付範囲選択
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime(2025, 8, 1),
            max_value=datetime.now()
        )
    
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime(2025, 8, 31),
            max_value=datetime.now()
        )
    
    if start_date > end_date:
        st.error("開始日は終了日より前である必要があります")
        return
    
    # データ読み込み
    df = load_kpi_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    revenue_data = load_revenue_breakdown(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    
    if df.empty:
        st.warning("指定された期間のデータが見つかりません")
        return
    
    # 売上内訳
    render_revenue_breakdown(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    
    st.divider()
    
    # KPIサマリー
    summary = calculate_kpi_summary(df)
    render_kpi_cards(summary)
    
    st.divider()
    
    # 売上トレンド
    render_revenue_trend(df)
    
    st.divider()
    
    # トラフィック指標
    st.subheader("📈 トラフィック指標")
    render_traffic_metrics(df)
    
    st.divider()
    
    # 効率指標
    st.subheader("⚡ 効率指標")
    render_efficiency_metrics(df)
    
    # データテーブル
    st.divider()
    st.subheader("📋 詳細データ")
    
    # 表示列を選択
    display_columns = [
        "date", "total_revenue", "sessions", "purchases", 
        "cost", "roas", "total_revenue_prev", "revenue_yoy_pct"
    ]
    
    available_columns = [col for col in display_columns if col in df.columns]
    display_df = df[available_columns].copy()
    
    # 数値列のフォーマット
    if "total_revenue" in display_df.columns:
        display_df["total_revenue"] = display_df["total_revenue"].apply(lambda x: f"¥{x:,.0f}")
    if "total_revenue_prev" in display_df.columns:
        display_df["total_revenue_prev"] = display_df["total_revenue_prev"].apply(lambda x: f"¥{x:,.0f}")
    if "cost" in display_df.columns:
        display_df["cost"] = display_df["cost"].apply(lambda x: f"¥{x:,.0f}")
    if "revenue_yoy_pct" in display_df.columns:
        display_df["revenue_yoy_pct"] = display_df["revenue_yoy_pct"].apply(lambda x: f"{x:+.1f}%")
    
    st.dataframe(display_df, use_container_width=True)
