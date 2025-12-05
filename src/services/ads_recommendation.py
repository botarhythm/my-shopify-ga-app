import pandas as pd
import streamlit as st

def generate_recommendations(df: pd.DataFrame) -> list:
    """
    Generate a list of recommendation dictionaries based on campaign performance.
    
    Rules:
    1. High ROAS (> 4.0) -> Increase Budget
    2. Wasted Spend (Cost > 10000 & Conversions == 0) -> Stop/Review
    3. Low CVR (Clicks > 100 & CVR < 1%) -> Check LP
    4. High CTR (> 2% & ROAS > 2.0) -> Good Creative
    """
    recommendations = []
    
    if df.empty:
        return recommendations

    for _, row in df.iterrows():
        campaign = row['campaign_name']
        cost = row['cost']
        clicks = row['clicks']
        conversions = row['conversions']
        roas = row['roas']
        
        # Calculate derived metrics safely
        cvr = (conversions / clicks * 100) if clicks > 0 else 0
        ctr = (clicks / row['impressions'] * 100) if row['impressions'] > 0 else 0

        # Rule 1: High ROAS
        if roas > 4.0:
            recommendations.append({
                "type": "positive",
                "campaign": campaign,
                "title": "💰 予算増額のチャンス",
                "message": f"キャンペーン「{campaign}」はROASが **{roas:.2f}** と非常に好調です。予算を増やしてさらに売上を伸ばしましょう。",
                "action": "予算設定を確認する"
            })

        # Rule 2: Wasted Spend
        if cost > 10000 and conversions == 0:
            recommendations.append({
                "type": "negative",
                "campaign": campaign,
                "title": "🛑 無駄な出費の可能性",
                "message": f"キャンペーン「{campaign}」は **¥{int(cost):,}** を消化しましたが、成果（コンバージョン）が出ていません。停止または設定の見直しを推奨します。",
                "action": "キャンペーンを停止/確認"
            })

        # Rule 3: Low CVR
        if clicks > 100 and cvr < 1.0:
            recommendations.append({
                "type": "warning",
                "campaign": campaign,
                "title": "📉 LP（飛び先）の改善が必要",
                "message": f"キャンペーン「{campaign}」はクリックされていますが、購入率（CVR）が **{cvr:.2f}%** と低めです。広告の飛び先ページが魅力的か確認しましょう。",
                "action": "LPを確認する"
            })

        # Rule 4: High CTR (Good Creative)
        if ctr > 2.0 and roas > 2.0:
             recommendations.append({
                "type": "positive",
                "campaign": campaign,
                "title": "✨ クリエイティブが好評です",
                "message": f"キャンペーン「{campaign}」のクリック率（CTR）が **{ctr:.2f}%** と高く、ユーザーの関心を惹けています。この広告文を他のキャンペーンでも参考にしましょう。",
                "action": "広告文を分析する"
            })
            
    return recommendations

def calculate_o2o_correlation(ads_df: pd.DataFrame, sales_df: pd.DataFrame) -> dict:
    """
    Calculate correlation between Ads metrics and Square sales.
    Expects daily aggregated DataFrames.
    """
    if ads_df.empty or sales_df.empty:
        return {"correlation": 0, "message": "データ不足のため分析できません"}

    # Merge on date
    # Ensure date columns are datetime
    ads_df['date'] = pd.to_datetime(ads_df['date'])
    sales_df['date'] = pd.to_datetime(sales_df['date'])
    
    merged = pd.merge(ads_df, sales_df, on='date', how='inner')
    
    if len(merged) < 5:
         return {"correlation": 0, "message": "データ点数が少なすぎます（5日以上必要）"}

    # Calculate correlation
    corr_cost = merged['cost'].corr(merged['square_sales'])
    
    message = ""
    if corr_cost > 0.7:
        message = "🔥 **非常に強い連動**: 広告費をかけると、店舗の売上も明確に伸びています！"
    elif corr_cost > 0.4:
        message = "📈 **緩やかな連動**: 広告費と店舗売上に正の相関が見られます。"
    elif corr_cost > -0.2:
        message = "🤔 **連動なし**: 広告と店舗売上の直接的な関係は見られません。"
    else:
        message = "📉 **逆相関**: 広告費が増えると店舗売上が下がる傾向（異常値の可能性あり）。"

    return {
        "correlation": corr_cost,
        "message": message,
        "merged_df": merged
    }
