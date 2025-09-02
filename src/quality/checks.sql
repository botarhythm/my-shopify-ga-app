-- src/quality/checks.sql
-- データ品質チェック

-- 欠損データチェック
-- 日別データで欠損がある日を検出
SELECT 
    'mart_daily' AS table_name,
    date,
    'Missing sessions' AS issue
FROM mart_daily 
WHERE sessions IS NULL

UNION ALL

SELECT 
    'mart_daily' AS table_name,
    date,
    'Missing total_revenue' AS issue
FROM mart_daily 
WHERE total_revenue IS NULL

UNION ALL

SELECT 
    'mart_daily' AS table_name,
    date,
    'Missing cost' AS issue
FROM mart_daily 
WHERE cost IS NULL;

-- 異常値チェック
-- セッション数の急変を検出（5標準偏差以上）
SELECT 
    date,
    sessions,
    LAG(sessions) OVER (ORDER BY date) AS prev_sessions,
    ABS(sessions - LAG(sessions) OVER (ORDER BY date)) AS change,
    STDDEV(sessions) OVER () AS stddev_sessions,
    'Large session change' AS issue
FROM mart_daily
WHERE ABS(sessions - LAG(sessions) OVER (ORDER BY date)) > 5 * STDDEV(sessions) OVER ();

-- 売上の異常値を検出
SELECT 
    date,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY date) AS prev_revenue,
    ABS(total_revenue - LAG(total_revenue) OVER (ORDER BY date)) AS change,
    STDDEV(total_revenue) OVER () AS stddev_revenue,
    'Large revenue change' AS issue
FROM mart_daily
WHERE ABS(total_revenue - LAG(total_revenue) OVER (ORDER BY date)) > 5 * STDDEV(total_revenue) OVER ();

-- データ整合性チェック
-- ROASが負の値になっていないか
SELECT 
    date,
    roas,
    'Negative ROAS' AS issue
FROM mart_daily
WHERE roas < 0;

-- セッション数が購入数より少ない異常値
SELECT 
    date,
    sessions,
    purchases,
    'Sessions less than purchases' AS issue
FROM mart_daily
WHERE sessions < purchases;

-- データ範囲チェック
-- 最新データが36時間以上古い場合
SELECT 
    MAX(date) AS latest_date,
    CURRENT_DATE - MAX(date) AS days_old,
    'Data too old' AS issue
FROM mart_daily
HAVING CURRENT_DATE - MAX(date) > 1;

-- 重複データチェック
-- 同じ日付の重複レコード
SELECT 
    date,
    COUNT(*) AS duplicate_count,
    'Duplicate date records' AS issue
FROM mart_daily
GROUP BY date
HAVING COUNT(*) > 1;

-- データ完全性チェック
-- 各テーブルのレコード数
SELECT 
    'stg_ga4' AS table_name,
    COUNT(*) AS record_count,
    MIN(date) AS min_date,
    MAX(date) AS max_date
FROM stg_ga4

UNION ALL

SELECT 
    'stg_shopify_orders' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM stg_shopify_orders

UNION ALL

SELECT 
    'stg_square_payments' AS table_name,
    COUNT(*) AS record_count,
    MIN(created_at) AS min_date,
    MAX(created_at) AS max_date
FROM stg_square_payments

UNION ALL

SELECT 
    'stg_ads_campaign' AS table_name,
    COUNT(*) AS record_count,
    MIN(date) AS min_date,
    MAX(date) AS max_date
FROM stg_ads_campaign;
