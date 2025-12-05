-- src/transform/build_yoy.sql
-- 前年同期（YoY）テーブル構築

-- 当年 vs 前年（同日）を結合
CREATE TABLE IF NOT EXISTS mart_daily_yoy AS
WITH current_year AS (
    SELECT * FROM mart_daily
),
previous_year AS (
    SELECT 
        date + INTERVAL '1 year' AS date, 
        * EXCLUDE(date) 
    FROM mart_daily
)
SELECT
    c.date,
    -- トラフィック
    c.sessions, 
    p.sessions AS sessions_prev,
    c.users, 
    p.users AS users_prev,
    c.purchases, 
    p.purchases AS purchases_prev,
    
    -- 売上
    c.total_revenue, 
    p.total_revenue AS total_revenue_prev,
    c.shopify_revenue, 
    p.shopify_revenue AS shopify_revenue_prev,
    c.square_revenue, 
    p.square_revenue AS square_revenue_prev,
    
    -- 広告
    c.cost, 
    p.cost AS cost_prev,
    c.clicks, 
    p.clicks AS clicks_prev,
    c.impressions, 
    p.impressions AS impressions_prev,
    c.conversions, 
    p.conversions AS conversions_prev,
    
    -- 効率指標
    c.roas, 
    p.roas AS roas_prev,
    
    -- YoY変化率
    CASE WHEN p.sessions > 0 THEN ((c.sessions - p.sessions) / p.sessions) * 100 ELSE NULL END AS sessions_yoy_pct,
    CASE WHEN p.users > 0 THEN ((c.users - p.users) / p.users) * 100 ELSE NULL END AS users_yoy_pct,
    CASE WHEN p.purchases > 0 THEN ((c.purchases - p.purchases) / p.purchases) * 100 ELSE NULL END AS purchases_yoy_pct,
    CASE WHEN p.total_revenue > 0 THEN ((c.total_revenue - p.total_revenue) / p.total_revenue) * 100 ELSE NULL END AS revenue_yoy_pct,
    CASE WHEN p.cost > 0 THEN ((c.cost - p.cost) / p.cost) * 100 ELSE NULL END AS cost_yoy_pct,
    CASE WHEN p.roas > 0 THEN ((c.roas - p.roas) / p.roas) * 100 ELSE NULL END AS roas_yoy_pct
FROM current_year c
LEFT JOIN previous_year p USING(date);

-- 商品別YoY
CREATE TABLE IF NOT EXISTS mart_product_yoy AS
WITH current_year AS (
    SELECT * FROM mart_product_daily
),
previous_year AS (
    SELECT 
        date + INTERVAL '1 year' AS date, 
        * EXCLUDE(date) 
    FROM mart_product_daily
)
SELECT
    c.date,
    c.title,
    c.sku,
    c.product_id,
    c.total_quantity,
    p.total_quantity AS total_quantity_prev,
    c.total_revenue,
    p.total_revenue AS total_revenue_prev,
    c.order_count,
    p.order_count AS order_count_prev,
    CASE WHEN p.total_quantity > 0 THEN ((c.total_quantity - p.total_quantity) / p.total_quantity) * 100 ELSE NULL END AS quantity_yoy_pct,
    CASE WHEN p.total_revenue > 0 THEN ((c.total_revenue - p.total_revenue) / p.total_revenue) * 100 ELSE NULL END AS revenue_yoy_pct
FROM current_year c
LEFT JOIN previous_year p USING(date, title, sku, product_id);

-- 流入元別YoY
CREATE TABLE IF NOT EXISTS mart_source_yoy AS
WITH current_year AS (
    SELECT * FROM mart_source_daily
),
previous_year AS (
    SELECT 
        date + INTERVAL '1 year' AS date, 
        * EXCLUDE(date) 
    FROM mart_source_daily
)
SELECT
    c.date,
    c.source,
    c.channel,
    c.sessions,
    p.sessions AS sessions_prev,
    c.cvr,
    p.cvr AS cvr_prev,
    c.revenue_per_session,
    p.revenue_per_session AS revenue_per_session_prev,
    CASE WHEN p.sessions > 0 THEN ((c.sessions - p.sessions) / p.sessions) * 100 ELSE NULL END AS sessions_yoy_pct,
    CASE WHEN p.cvr > 0 THEN ((c.cvr - p.cvr) / p.cvr) * 100 ELSE NULL END AS cvr_yoy_pct
FROM current_year c
LEFT JOIN previous_year p USING(date, source, channel);

-- キャンペーン別YoY
CREATE TABLE IF NOT EXISTS mart_campaign_yoy AS
WITH current_year AS (
    SELECT * FROM mart_campaign_daily
),
previous_year AS (
    SELECT 
        date + INTERVAL '1 year' AS date, 
        * EXCLUDE(date) 
    FROM mart_campaign_daily
)
SELECT
    c.date,
    c.campaign_id,
    c.campaign_name,
    c.cost,
    p.cost AS cost_prev,
    c.roas,
    p.roas AS roas_prev,
    c.ctr,
    p.ctr AS ctr_prev,
    c.cvr,
    p.cvr AS cvr_prev,
    CASE WHEN p.cost > 0 THEN ((c.cost - p.cost) / p.cost) * 100 ELSE NULL END AS cost_yoy_pct,
    CASE WHEN p.roas > 0 THEN ((c.roas - p.roas) / p.roas) * 100 ELSE NULL END AS roas_yoy_pct
FROM current_year c
LEFT JOIN previous_year p USING(date, campaign_id, campaign_name);
