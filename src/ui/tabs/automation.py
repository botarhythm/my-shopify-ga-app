import streamlit as st
import pandas as pd
from src.services.inventory_service import inventory_service
from src.services.audience_service import audience_service

def render_automation_tab():
    st.header("🤖 広告自動化 & 顧客連携 (Ad Automation)")
    
    st.markdown("""
    ここでは、Google広告の運用を自動化したり、店舗の顧客データを広告に活用するための設定ができます。
    専門的な知識がなくても、ボタン一つで効率的な運用が可能です。
    """)
    
    # 1. Inventory-Aware Ads
    st.markdown("---")
    st.subheader("📦 在庫連動型 広告停止 (Inventory-Aware Ads)")
    
    with st.expander("ℹ️ この機能について詳しく見る"):
        st.markdown("""
        **機能の概要**:
        Shopifyの在庫状況をチェックし、在庫切れになっている商品のGoogle広告を自動的に「一時停止」します。
        
        **なぜ重要？**:
        在庫がない商品の広告を出し続けると、クリックされても購入できず、広告費が無駄になってしまいます（Wasted Spend）。
        この機能を使うことで、無駄な出費を抑え、在庫がある商品に予算を集中できます。
        
        **仕組み**:
        1. Shopifyから全商品の在庫数を取得します。
        2. Google広告のキャンペーン名と商品名を照合します。
        3. 在庫が設定値以下の場合、そのキャンペーンを停止します。
        """)

    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("#### 設定")
        threshold = st.number_input(
            "在庫数のしきい値", 
            min_value=0, 
            value=0, 
            help="在庫がこの数以下になったら広告を停止します（通常は0）。"
        )
        
        dry_run_inventory = st.checkbox(
            "テスト実行 (Dry Run)", 
            value=True, 
            help="チェックを入れると、実際には広告を停止せず、停止対象のリストアップだけを行います。最初はチェックを入れたまま確認することをお勧めします。"
        )
        
        if st.button("在庫チェックを実行", type="primary"):
            with st.spinner("ShopifyとGoogle広告のデータを照合中..."):
                # Pass the user-defined threshold to the service
                logs = inventory_service.check_and_update_ads(dry_run=dry_run_inventory, threshold=threshold)
                
                if logs:
                    st.success("チェック完了")
                    
                    # Process logs for display
                    df_logs = pd.DataFrame(logs)
                    
                    # Translate columns for display
                    display_cols = {
                        "campaign_name": "キャンペーン名",
                        "product": "対象商品",
                        "inventory": "現在庫数",
                        "action": "判定",
                        "result": "実行結果",
                        "status": "状態",
                        "message": "メッセージ"
                    }
                    df_display = df_logs.rename(columns=display_cols)
                    
                    st.dataframe(df_display, use_container_width=True)
                    
                    # Summary
                    paused_count = len([l for l in logs if l.get('action') == 'PAUSE'])
                    if paused_count > 0:
                        st.warning(f"⚠️ {paused_count} 件のキャンペーンが停止対象です。")
                    else:
                        st.info("✅ 停止が必要なキャンペーンはありませんでした（すべて在庫あり、または該当なし）。")
                else:
                    st.info("ログはありません。")

    with col2:
        st.info("💡 **ヒント**: キャンペーン名に商品名を含めておくと、自動的に紐付けが行われます。")

    # 2. Customer Match
    st.markdown("---")
    st.subheader("👥 優良顧客リスト連携 (Customer Match)")
    
    with st.expander("ℹ️ この機能について詳しく見る"):
        st.markdown("""
        **機能の概要**:
        ShopifyとSquareの購入データから「優良顧客（VIP）」を抽出し、Google広告に連携します。
        
        **メリット**:
        *   **類似配信**: VIP顧客に似た特徴を持つ「新規ユーザー」に広告を配信でき、質の高い集客が期待できます。
        *   **除外設定**: 既に購入済みのVIP顧客には広告を出さないようにして、無駄なコストを削減できます。
        
        **プライバシー**:
        メールアドレスなどの個人情報は、送信前にハッシュ化（暗号化のような処理）されるため、安全に連携されます。
        """)

    col3, col4 = st.columns([1, 2])
    
    with col3:
        st.markdown("#### 設定")
        vip_spend_threshold = st.number_input(
            "VIP認定金額 (円)", 
            min_value=1000, 
            value=10000, 
            step=1000,
            help="これ以上の購入金額がある顧客をVIPとして抽出します。"
        )
        
        dry_run_audience = st.checkbox(
            "テスト実行 (Dry Run) ", 
            value=True, 
            key="dry_run_audience",
            help="チェックを入れると、Google広告へのアップロードは行わず、対象人数の確認だけを行います。"
        )
        
        if st.button("リスト連携を実行", type="primary"):
            with st.spinner("顧客データを抽出・加工中..."):
                try:
                    # Pass the user-defined threshold
                    audience_service.sync_vip_customers(dry_run=dry_run_audience, spend_threshold=vip_spend_threshold)
                    st.success("連携処理が完了しました！詳細はターミナルログを確認してください。")
                    st.info("※ 本番運用では、ここに詳細な連携結果（成功件数など）が表示されます。")
                except Exception as e:
                    st.error(f"エラーが発生しました: {e}")

    with col4:
        st.info("💡 **ヒント**: 定期的にこのボタンを押すことで、常に最新の顧客リストを広告に活用できます。")
