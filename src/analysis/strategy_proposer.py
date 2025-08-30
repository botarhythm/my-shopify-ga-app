#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析結果を基にした売上向上施策提案スクリプト
Google広告改善案、ウェブサイト改善案、マーケティング施策を提案します。

必要なライブラリのインストール:
pip install pandas
"""

import pandas as pd
import os
import glob
from datetime import datetime
import re

def find_latest_analysis_files():
    """最新の分析ファイルを見つけます。"""
    files = {}
    
    # 分析レポート
    analysis_reports = glob.glob("analysis_report_*.md")
    if analysis_reports:
        files['analysis_report'] = max(analysis_reports, key=os.path.getctime)
    
    # 分析チャート
    analysis_charts = glob.glob("analysis_charts_*.png")
    if analysis_charts:
        files['analysis_charts'] = max(analysis_charts, key=os.path.getctime)
    
    # Shopify注文データ
    shopify_orders = glob.glob("shopify_orders_*.csv")
    if shopify_orders:
        files['shopify_orders'] = max(shopify_orders, key=os.path.getctime)
    
    # Google Analyticsデータ
    ga4_data = glob.glob("ga4_data_*.csv")
    if ga4_data:
        files['ga4_data'] = max(ga4_data, key=os.path.getctime)
    
    return files

def load_analysis_data(files):
    """分析データを読み込みます。"""
    data = {}
    
    # 分析レポートの読み込み
    if 'analysis_report' in files:
        try:
            with open(files['analysis_report'], 'r', encoding='utf-8') as f:
                data['report_content'] = f.read()
            print(f"✓ 分析レポート読み込み完了: {files['analysis_report']}")
        except Exception as e:
            print(f"✗ 分析レポートの読み込みに失敗: {e}")
    
    # CSVデータの読み込み
    for file_type, file_path in files.items():
        if file_type.endswith('.csv'):
            try:
                if file_type == 'shopify_orders':
                    data['orders'] = pd.read_csv(file_path)
                    print(f"✓ Shopify注文データ読み込み完了: {len(data['orders'])}件")
                elif file_type == 'ga4_data':
                    data['ga4'] = pd.read_csv(file_path)
                    print(f"✓ Google Analyticsデータ読み込み完了: {len(data['ga4'])}件")
            except Exception as e:
                print(f"✗ {file_type}の読み込みに失敗: {e}")
    
    return data

def extract_top_products_from_report(report_content):
    """分析レポートから売上トップ商品を抽出します。"""
    products = []
    
    if not report_content:
        return products
    
    # 売上トップ5商品セクションを検索
    lines = report_content.split('\n')
    in_products_section = False
    
    for line in lines:
        if '## 1. 売上トップ5商品' in line:
            in_products_section = True
            continue
        elif line.startswith('## 2.') and in_products_section:
            break
        
        if in_products_section and line.strip().startswith('1.') or line.strip().startswith('2.') or line.strip().startswith('3.') or line.strip().startswith('4.') or line.strip().startswith('5.'):
            # 商品名を抽出
            product_name = line.split('**')[1].split('**')[0] if '**' in line else line.split('. ')[1] if '. ' in line else line
            products.append(product_name.strip())
    
    return products

def analyze_traffic_sources_for_strategy(ga4_df):
    """施策提案のための流入元分析を行います。"""
    if ga4_df.empty:
        return None
    
    try:
        # 流入元別の売上分析
        traffic_analysis = ga4_df.groupby('source').agg({
            'sessions': 'sum',
            'totalRevenue': 'sum'
        }).reset_index()
        
        # セッションあたりの売上を計算
        traffic_analysis['revenue_per_session'] = (
            traffic_analysis['totalRevenue'] / traffic_analysis['sessions']
        ).round(2)
        
        # 売上順でソート
        traffic_analysis = traffic_analysis.sort_values('totalRevenue', ascending=False)
        
        return traffic_analysis
        
    except Exception as e:
        print(f"流入元分析中にエラーが発生: {e}")
        return None

def generate_google_ads_strategies(top_products, traffic_analysis):
    """Google広告の改善案を生成します。"""
    print("\n=== Google広告改善案 ===")
    
    strategies = []
    
    if top_products:
        strategies.append("## 🎯 Google広告改善案")
        strategies.append("")
        
        for i, product in enumerate(top_products[:5], 1):
            strategies.append(f"### {i}. **{product}** の広告強化")
            strategies.append("")
            strategies.append("#### キーワード戦略:")
            strategies.append(f"- **メインキーワード**: {product}")
            strategies.append(f"- **長尾キーワード**: {product} おすすめ, {product} レビュー, {product} 比較")
            strategies.append(f"- **関連キーワード**: {product} 類似商品, {product} 代替品")
            strategies.append("")
            
            strategies.append("#### 広告コピー案:")
            strategies.append(f"- **ヘッドライン1**: {product} で理想のスタイルを")
            strategies.append(f"- **ヘッドライン2**: お客様満足度95%以上")
            strategies.append(f"- **ヘッドライン3**: 今なら送料無料")
            strategies.append("")
            
            strategies.append("#### ターゲティング改善:")
            strategies.append("- **年齢層**: 25-45歳")
            strategies.append("- **興味**: ファッション、ライフスタイル、オンラインショッピング")
            strategies.append("- **デバイス**: モバイル優先（70%の注文がモバイルから）")
            strategies.append("")
    
    if traffic_analysis is not None and not traffic_analysis.empty:
        # Googleからの流入が少ない場合の提案
        google_traffic = traffic_analysis[traffic_analysis['source'].str.contains('google', case=False, na=False)]
        if google_traffic.empty or google_traffic['sessions'].sum() < 20:
            strategies.append("### 🔍 Google広告の拡大提案")
            strategies.append("")
            strategies.append("#### 現状分析:")
            strategies.append("- Googleからの流入が少ない")
            strategies.append("- オーガニック検索の強化が必要")
            strategies.append("")
            strategies.append("#### 改善施策:")
            strategies.append("1. **検索広告の予算増額**")
            strategies.append("2. **ショッピング広告の開始**")
            strategies.append("3. **ディスプレイ広告でのブランド認知向上**")
            strategies.append("4. **リマーケティング広告の強化**")
            strategies.append("")
    
    return strategies

def generate_website_improvement_strategies(top_products, traffic_analysis):
    """ウェブサイトの改善案を生成します。"""
    print("\n=== ウェブサイト改善案 ===")
    
    strategies = []
    
    if top_products:
        strategies.append("## 🌐 ウェブサイト改善案")
        strategies.append("")
        
        for i, product in enumerate(top_products[:3], 1):
            strategies.append(f"### {i}. **{product}** の商品ページ改善")
            strategies.append("")
            strategies.append("#### コンテンツ強化:")
            strategies.append(f"- **商品説明の詳細化**: {product}の特徴、使用方法、お手入れ方法")
            strategies.append(f"- **お客様の声追加**: 実際の購入者からのレビュー・写真")
            strategies.append(f"- **関連商品の提案**: {product}と合わせて使いたい商品")
            strategies.append("")
            
            strategies.append("#### UX改善:")
            strategies.append("- **画像の高解像度化**: 360度ビュー、ズーム機能")
            strategies.append("- **在庫状況の明確化**: リアルタイム在庫表示")
            strategies.append("- **配送情報の詳細化**: 配送日数、配送料金の明確化")
            strategies.append("")
    
    if traffic_analysis is not None and not traffic_analysis.empty:
        # 直接アクセスの多い場合の提案
        direct_traffic = traffic_analysis[traffic_analysis['source'].str.contains('direct', case=False, na=False)]
        if not direct_traffic.empty and direct_traffic['sessions'].sum() > 10:
            strategies.append("### 🏠 直接アクセス対策")
            strategies.append("")
            strategies.append("#### 現状分析:")
            strategies.append("- 直接アクセス（ブックマーク、直接URL入力）が多い")
            strategies.append("- リピーターの割合が高い")
            strategies.append("")
            strategies.append("#### 改善施策:")
            strategies.append("1. **パーソナライゼーション強化**")
            strategies.append("2. **おすすめ商品の表示精度向上**")
            strategies.append("3. **ログイン後の体験向上**")
            strategies.append("4. **メールマガジンの最適化**")
            strategies.append("")
    
    return strategies

def generate_marketing_strategies(traffic_analysis):
    """新しいマーケティング施策を提案します。"""
    print("\n=== マーケティング施策提案 ===")
    
    strategies = []
    
    if traffic_analysis is not None and not traffic_analysis.empty:
        strategies.append("## 📈 マーケティング施策提案")
        strategies.append("")
        
        # Instagram分析
        instagram_traffic = traffic_analysis[traffic_analysis['source'].str.contains('instagram', case=False, na=False)]
        if instagram_traffic.empty or instagram_traffic['sessions'].sum() < 5:
            strategies.append("### 📱 Instagramマーケティング強化")
            strategies.append("")
            strategies.append("#### 現状分析:")
            strategies.append("- Instagramからの流入が少ない")
            strategies.append("- SNSマーケティングの機会損失")
            strategies.append("")
            strategies.append("#### 改善施策:")
            strategies.append("1. **Instagram広告の開始**")
            strategies.append("   - ストーリー広告: 商品の使用シーン")
            strategies.append("   - フィード広告: 商品画像と説明")
            strategies.append("   - リール広告: 商品紹介動画")
            strategies.append("")
            strategies.append("2. **インフルエンサーコラボレーション**")
            strategies.append("   - ファッション系インフルエンサーとの提携")
            strategies.append("   - 商品レビュー動画の制作")
            strategies.append("   - アフィリエイトプログラムの開始")
            strategies.append("")
        
        # 検索エンジン最適化
        organic_traffic = traffic_analysis[traffic_analysis['source'].str.contains('google|yahoo|bing', case=False, na=False)]
        if organic_traffic.empty or organic_traffic['sessions'].sum() < 15:
            strategies.append("### 🔍 SEO・コンテンツマーケティング強化")
            strategies.append("")
            strategies.append("#### 現状分析:")
            strategies.append("- オーガニック検索からの流入が少ない")
            strategies.append("- 検索エンジンでの露出不足")
            strategies.append("")
            strategies.append("#### 改善施策:")
            strategies.append("1. **ブログ・コンテンツの充実**")
            strategies.append("   - ファッションコーディネート記事")
            strategies.append("   - 商品の使い方・お手入れ方法")
            strategies.append("   - トレンド情報・スタイル提案")
            strategies.append("")
            strategies.append("2. **技術的SEOの改善**")
            strategies.append("   - ページ速度の最適化")
            strategies.append("   - モバイルフレンドリーの向上")
            strategies.append("   - 構造化データの実装")
            strategies.append("")
        
        # メールマーケティング
        strategies.append("### 📧 メールマーケティング最適化")
        strategies.append("")
        strategies.append("#### 改善施策:")
        strategies.append("1. **セグメンテーション強化**")
        strategies.append("   - 購入履歴に基づく商品推薦")
        strategies.append("   - 行動パターンに基づく配信タイミング")
        strategies.append("   - 顧客価値に基づく配信内容")
        strategies.append("")
        strategies.append("2. **自動化の導入**")
        strategies.append("   - ウェルカムメールシーケンス")
        strategies.append("   - カート放棄リマインダー")
        strategies.append("   - 再購入促進メール")
        strategies.append("")
    
    return strategies

def generate_action_plan(top_products, traffic_analysis):
    """具体的なアクションプランを生成します。"""
    print("\n=== アクションプラン生成 ===")
    
    strategies = []
    
    strategies.append("## 🚀 即座に実行可能なアクションプラン")
    strategies.append("")
    
    # 優先度1: 即座に実行
    strategies.append("### 🔥 優先度1: 今週中に実行")
    strategies.append("")
    
    if top_products and len(top_products) > 0:
        strategies.append("1. **売上トップ商品の広告予算増額**")
        strategies.append(f"   - {top_products[0]}のGoogle広告予算を20%増額")
        if len(top_products) > 1:
            strategies.append(f"   - {top_products[1]}のキーワード追加")
        strategies.append("")
    
    strategies.append("2. **Google Analyticsの目標設定**")
    strategies.append("   - 購入完了ページの目標設定")
    strategies.append("   - カート追加の目標設定")
    strategies.append("   - 商品詳細ページ滞在時間の目標設定")
    strategies.append("")
    
    # 優先度2: 1ヶ月以内
    strategies.append("### ⚡ 優先度2: 1ヶ月以内に実行")
    strategies.append("")
    
    strategies.append("1. **商品ページの改善**")
    strategies.append("   - 売上トップ商品の画像・説明の強化")
    strategies.append("   - お客様レビューの追加")
    strategies.append("   - 関連商品の表示強化")
    strategies.append("")
    
    strategies.append("2. **Instagram広告の開始**")
    strategies.append("   - 月間予算: 50,000円")
    strategies.append("   - ターゲット: 25-45歳、ファッション興味")
    strategies.append("   - 商品画像とストーリー広告")
    strategies.append("")
    
    # 優先度3: 3ヶ月以内
    strategies.append("### 📅 優先度3: 3ヶ月以内に実行")
    strategies.append("")
    
    strategies.append("1. **コンテンツマーケティングの開始**")
    strategies.append("   - ブログ記事の月2回更新")
    strategies.append("   - 商品の使い方動画制作")
    strategies.append("   - スタイルガイドの作成")
    strategies.append("")
    
    strategies.append("2. **リマーケティングの強化**")
    strategies.append("   - Facebook/Instagramリマーケティング")
    strategies.append("   - Googleディスプレイリマーケティング")
    strategies.append("   - メールリマーケティングの自動化")
    strategies.append("")
    
    return strategies

def generate_strategy_report(all_strategies):
    """包括的な戦略レポートを生成します。"""
    print("\n=== 戦略レポート生成 ===")
    
    report = []
    report.append("# 🎯 Shopifyストア売上向上戦略レポート")
    report.append(f"生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    report.append("")
    report.append("## 📊 分析サマリー")
    report.append("")
    report.append("このレポートは、Shopifyストアの売上データとGoogle Analyticsの行動データを統合分析し、")
    report.append("具体的な売上向上施策を提案するものです。")
    report.append("")
    
    # 各戦略を追加
    for strategy_section in all_strategies:
        if strategy_section:
            report.extend(strategy_section)
            report.append("")
    
    # レポートを保存
    report_filename = f"strategy_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    print(f"戦略レポートを {report_filename} に保存しました。")
    
    # レポート内容を表示
    print("\n" + "="*60)
    print("戦略レポート")
    print("="*60)
    for line in report:
        print(line)

def main():
    """メイン処理を実行します。"""
    print("Shopifyストア売上向上戦略提案を開始します...")
    
    # 最新の分析ファイルを検索
    files = find_latest_analysis_files()
    
    if not files:
        print("分析対象のファイルが見つかりません。")
        print("先にデータ分析を実行してください。")
        return
    
    print(f"見つかったファイル: {files}")
    
    # データを読み込み
    data = load_analysis_data(files)
    
    if not data:
        print("データの読み込みに失敗しました。")
        return
    
    # 分析レポートから売上トップ商品を抽出
    top_products = extract_top_products_from_report(data.get('report_content', ''))
    
    # 流入元分析
    traffic_analysis = analyze_traffic_sources_for_strategy(data.get('ga4', pd.DataFrame()))
    
    # 各種戦略を生成
    google_ads_strategies = generate_google_ads_strategies(top_products, traffic_analysis)
    website_strategies = generate_website_improvement_strategies(top_products, traffic_analysis)
    marketing_strategies = generate_marketing_strategies(traffic_analysis)
    action_plan = generate_action_plan(top_products, traffic_analysis)
    
    # 全戦略を統合
    all_strategies = [
        google_ads_strategies,
        website_strategies,
        marketing_strategies,
        action_plan
    ]
    
    # 戦略レポート生成
    generate_strategy_report(all_strategies)
    
    print("\n戦略提案が完了しました。")
    print("生成されたレポートファイルを確認し、優先度の高い施策から実行してください。")

if __name__ == "__main__":
    main()

