
# Marketing Terminology Glossary for Beginners

TERMS = {
    "Total Sales": {
        "label": "売上総額",
        "desc": "Shopifyから取得した実際の注文金額の合計です。キャンセルや返品は除外されている場合があります（設定によります）。\n※この値には広告経由以外の売上（自然検索、直接アクセス、リピーター等）も含まれます。"
    },
    "Ad Attributed Sales": {
        "label": "広告経由売上",
        "desc": "Google広告経由で発生した売上のみです。広告をクリックしたユーザーが購入に至った金額の合計です。\nROAS計算にはこの値を使用します。"
    },
    "Ad Spend": {
        "label": "広告費",
        "desc": "Google広告などで使用した費用の合計です。"
    },
    "ROAS": {
        "label": "ROAS (広告費用対効果)",
        "desc": "Return On Ad Spendの略。広告費1円あたり、何円の売上があったかを示します。\n計算式: 広告経由売上 ÷ 広告費\n例: ROAS 2.0 = 100万円の広告費で200万円の広告経由売上。\n数値が高いほど広告の効率が良いことを意味します。\n\n※注意: 全売上ではなく、広告経由の売上のみを使用して計算しています。"
    },
    "Sessions": {
        "label": "セッション数",
        "desc": "Webサイトへの訪問回数です。同じ人が朝と夜に1回ずつ訪問した場合、2セッションとカウントされます。"
    },
    "Campaign": {
        "label": "キャンペーン",
        "desc": "広告を管理する大きなまとまりです。例えば「夏セール」「新商品プロモーション」などの目的別に作成されます。"
    },
    "Cost": {
        "label": "費用",
        "desc": "その広告キャンペーンにかかった費用です。"
    },
    "Conversions": {
        "label": "コンバージョン数 (CV)",
        "desc": "広告をクリックしたユーザーが、購入や問い合わせなどの「成果」に至った回数です。"
    },
    "Conversions Value": {
        "label": "コンバージョン値",
        "desc": "コンバージョンによって発生した価値（主に売上金額）の合計です。"
    },
    "CTR": {
        "label": "クリック率 (CTR)",
        "desc": "Click Through Rateの略。広告が表示された回数のうち、実際にクリックされた割合です。\n計算式: クリック数 ÷ 表示回数"
    },
    "CPC": {
        "label": "クリック単価 (CPC)",
        "desc": "Cost Per Clickの略。広告1クリックあたりにかかった平均費用です。"
    }
}

def get_term_help(term_key):
    """Returns the description for a given term key."""
    if term_key in TERMS:
        return TERMS[term_key]["desc"]
    return ""

def get_term_label(term_key):
    """Returns the friendly label for a given term key."""
    if term_key in TERMS:
        return TERMS[term_key]["label"]
    return term_key
