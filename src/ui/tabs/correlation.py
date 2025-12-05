import streamlit as st
import pandas as pd
import plotly.express as px
from src.services.correlation_service import correlation_service

def render_correlation_tab():
    st.title("📊 O2O相関分析 (広告 × 店舗売上)")
    
    with st.expander("ℹ️ この分析について"):
        st.markdown("""
        **目的**: 「Google広告を出した日」と「その後の店舗売上」の関係を統計的に分析します。
        
        **仕組み**:
        *   広告費の変動と、店舗売上（Square）の変動パターンを比較します。
        *   「広告を出した当日」「1日後」「2日後」...と時間をずらしながら相関係数を計算します。
        *   相関係数が高いタイミング = 広告の効果が現れるまでの期間（タイムラグ）です。
        
        **読み方**:
        *   **相関係数 (r)**: -1.0 〜 +1.0 の値。+1に近いほど「広告費が増えると売上も増える」関係が強い。
        *   **Lag (日数)**: 広告を出してから何日後の売上を見ているか。
        """)

    st.markdown("---")
    
    # Run Analysis
    with st.spinner("過去90日間のデータを分析中..."):
        try:
            df = correlation_service.analyze_ad_store_correlation(max_lag=7)
            
            if df.empty:
                st.warning("分析に十分なデータがありません。ETLを実行してデータを取得してください。")
                return
            
            # Find max correlation
            max_corr_row = df.loc[df['Correlation'].idxmax()]
            max_lag = int(max_corr_row['Lag (Days)'])
            max_corr = max_corr_row['Correlation']
            
            # Display Key Finding
            st.subheader("🎯 分析結果")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    label="最も強い相関のタイミング",
                    value=f"{max_lag}日後",
                    help="広告を出してから、この日数後の店舗売上と最も相関が高い"
                )
            
            with col2:
                st.metric(
                    label="相関係数 (r)",
                    value=f"{max_corr:.3f}",
                    help="1.0に近いほど強い正の相関（広告↑ → 売上↑）"
                )
            
            with col3:
                # Interpretation
                if max_corr > 0.5:
                    strength = "強い"
                    color = "🟢"
                elif max_corr > 0.3:
                    strength = "中程度"
                    color = "🟡"
                elif max_corr > 0:
                    strength = "弱い"
                    color = "🟠"
                else:
                    strength = "なし/逆相関"
                    color = "🔴"
                    
                st.metric(
                    label="相関の強さ",
                    value=f"{color} {strength}"
                )
            
            # Interpretation Text
            st.markdown("---")
            st.markdown("### 💡 解釈")
            
            if max_corr > 0.3:
                st.success(f"""
                **広告の効果が確認できます！**
                
                広告を出した **{max_lag}日後** に店舗売上が増える傾向が見られます（相関係数: {max_corr:.3f}）。
                これは、お客様が広告を見てから実際に来店するまでに約{max_lag}日かかっている可能性を示唆しています。
                
                **推奨アクション**:
                *   週末の来店を狙うなら、{max_lag}日前（平日）に広告を強化する。
                *   広告効果の測定は、配信当日ではなく{max_lag}日後の売上まで含めて評価する。
                """)
            elif max_corr > 0:
                st.info(f"""
                **弱い相関が見られます。**
                
                広告と店舗売上の間に弱い関係性が見られますが、他の要因（天候、イベントなど）の影響も大きい可能性があります。
                
                **推奨アクション**:
                *   より長期間のデータで再分析する。
                *   曜日や季節要因を考慮した高度な分析を検討する。
                """)
            else:
                st.warning("""
                **明確な相関が見られません。**
                
                現在のデータでは、広告費と店舗売上の間に統計的な関係性が確認できませんでした。
                
                **考えられる理由**:
                *   広告が主にオンライン購入を促進している（店舗ではなくShopifyでの購入）。
                *   データ期間が短い、または広告費の変動が少ない。
                *   店舗売上は広告以外の要因（口コミ、立地など）に強く依存している。
                """)
            
            # Visualization
            st.markdown("---")
            st.subheader("📈 タイムラグ別の相関")
            
            fig = px.bar(
                df,
                x='Lag (Days)',
                y='Correlation',
                title='広告費と店舗売上の相関（タイムラグ別）',
                labels={'Lag (Days)': '広告配信からの日数', 'Correlation': '相関係数'},
                color='Correlation',
                color_continuous_scale=['red', 'yellow', 'green']
            )
            
            fig.update_layout(
                xaxis=dict(tickmode='linear', tick0=0, dtick=1),
                yaxis=dict(range=[-1, 1])
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Data Table
            with st.expander("📋 詳細データを見る"):
                st.dataframe(df, use_container_width=True)
                
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
