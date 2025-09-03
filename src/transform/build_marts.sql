-- src/transform/build_marts.sql
-- マートテーブル構築（core → marts）

-- 売上（日別）
CREATE TABLE IF NOT EXISTS mart_revenue_daily AS
SELECT 
    d::DATE AS date,
    COALESCE(shop_rev, 0) + COALESCE(sq_rev, 0) AS total_revenue,
    COALESCE(shop_rev, 0) AS shopify_revenue,
    COALESCE(sq_rev, 0) AS square_revenue
FROM (
    SELECT DISTINCT date_trunc('day', date) AS d FROM core_ga4
    UNION
    SELECT DISTINCT date FROM core_shopify_orders
    UNION
    SELECT DISTINCT date FROM core_square_payments
) days
LEFT JOIN (
    SELECT date, SUM(order_total) AS shop_rev 
    FROM core_shopify_orders 
    GROUP BY 1
) s ON s.date = days.d
LEFT JOIN (
    SELECT date, SUM(amount) AS sq_rev 
    FROM core_square_payments 
    GROUP BY 1
) q ON q.date = days.d;

-- トラフィック（日別）
CREATE TABLE IF NOT EXISTS mart_traffic_daily AS
SELECT 
    date, 
    SUM(sessions) AS sessions, 
    SUM(users) AS users, 
    SUM(purchases) AS purchases, 
    SUM(revenue) AS ga_revenue
FROM core_ga4 
GROUP BY 1;

-- 広告（日別）
CREATE TABLE IF NOT EXISTS mart_ads_daily AS
SELECT 
    date, 
    SUM(cost) AS cost, 
    SUM(clicks) AS clicks, 
    SUM(impressions) AS impressions,
    SUM(conversions) AS conversions, 
    SUM(conversions_value) AS conversions_value
FROM core_ads_campaign 
GROUP BY 1;

-- 統合（日別）
CREATE TABLE IF NOT EXISTS mart_daily AS
SELECT 
    d.date,
    t.sessions, 
    t.users, 
    t.purchases, 
    t.ga_revenue,
    r.total_revenue, 
    r.shopify_revenue, 
    r.square_revenue,
    a.cost, 
    a.clicks, 
    a.impressions, 
    a.conversions, 
    a.conv_value,
    CASE WHEN a.cost > 0 THEN (r.total_revenue / a.cost) ELSE NULL END AS roas
FROM (SELECT DISTINCT date FROM mart_revenue_daily) d
LEFT JOIN mart_traffic_daily t USING(date)
LEFT JOIN mart_revenue_daily r USING(date)
LEFT JOIN mart_ads_daily a USING(date);

-- 商品別売上（日別）
CREATE TABLE IF NOT EXISTS mart_product_daily AS
SELECT 
    date,
    title,
    sku,
    product_id,
    SUM(quantity) AS total_quantity,
    SUM(price * quantity) AS total_revenue,
    COUNT(DISTINCT order_id) AS order_count
FROM core_shopify_orders
GROUP BY 1, 2, 3, 4;

-- 流入元別効率（日別）
CREATE TABLE IF NOT EXISTS mart_source_daily AS
SELECT 
    date,
    source,
    channel,
    SUM(sessions) AS sessions,
    SUM(users) AS users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS ga_revenue,
    CASE WHEN SUM(sessions) > 0 THEN (SUM(purchases) / SUM(sessions)) ELSE 0 END AS cvr,
    CASE WHEN SUM(sessions) > 0 THEN (SUM(revenue) / SUM(sessions)) ELSE 0 END AS revenue_per_session
FROM core_ga4
GROUP BY 1, 2, 3;

-- ページ別効率（日別）
CREATE TABLE IF NOT EXISTS mart_page_daily AS
SELECT 
    date,
    page_path,
    SUM(sessions) AS sessions,
    SUM(users) AS users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS ga_revenue,
    CASE WHEN SUM(sessions) > 0 THEN (SUM(purchases) / SUM(sessions)) ELSE 0 END AS cvr,
    CASE WHEN SUM(sessions) > 0 THEN (SUM(revenue) / SUM(sessions)) ELSE 0 END AS revenue_per_session
FROM core_ga4
GROUP BY 1, 2;

-- キャンペーン別効率（日別）
CREATE TABLE IF NOT EXISTS mart_campaign_daily AS
SELECT 
    date,
    campaign_id,
    campaign_name,
    SUM(cost) AS cost,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(conversions) AS conversions,
    SUM(conversions_value) AS conversions_value,
    CASE WHEN SUM(impressions) > 0 THEN (SUM(clicks) / SUM(impressions)) ELSE 0 END AS ctr,
    CASE WHEN SUM(clicks) > 0 THEN (SUM(conversions) / SUM(clicks)) ELSE 0 END AS cvr,
    CASE WHEN SUM(cost) > 0 THEN (SUM(conversions_value) / SUM(cost)) ELSE 0 END AS roas
FROM core_ads_campaign
GROUP BY 1, 2, 3;

-- キーワード別効率（日別）
CREATE TABLE IF NOT EXISTS mart_keyword_daily AS
SELECT 
    date,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    keyword,
    SUM(cost) AS cost,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(conversions) AS conversions,
    SUM(conversions_value) AS conversions_value,
    CASE WHEN SUM(impressions) > 0 THEN (SUM(clicks) / SUM(impressions)) ELSE 0 END AS ctr,
    CASE WHEN SUM(clicks) > 0 THEN (SUM(conversions) / SUM(clicks)) ELSE 0 END AS cvr,
    CASE WHEN SUM(cost) > 0 THEN (SUM(conversions_value) / SUM(cost)) ELSE 0 END AS roas
FROM core_ads_keyword
GROUP BY 1, 2, 3, 4, 5, 6;
