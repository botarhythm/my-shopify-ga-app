def render_traffic_metrics(df: pd.DataFrame):
    """トラフィック指標を表示"""
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

