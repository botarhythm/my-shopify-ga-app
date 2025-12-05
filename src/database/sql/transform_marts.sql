-- Transform Core to Marts

-- Daily Revenue (Shopify + Square)
CREATE OR REPLACE TABLE marts.daily_revenue AS
WITH shopify AS (
    SELECT 
        date, 
        SUM(total_price) as shopify_sales,
        COUNT(DISTINCT order_id) as shopify_orders
    FROM core.shopify_orders
    GROUP BY 1
),
square AS (
    SELECT 
        date, 
        SUM(amount) as square_sales,
        COUNT(DISTINCT payment_id) as square_txns
    FROM core.square_payments
    GROUP BY 1
)
SELECT 
    COALESCE(s.date, q.date) as date,
    COALESCE(s.shopify_sales, 0) as shopify_sales,
    COALESCE(s.shopify_orders, 0) as shopify_orders,
    COALESCE(q.square_sales, 0) as square_sales,
    COALESCE(q.square_txns, 0) as square_txns,
    COALESCE(s.shopify_sales, 0) + COALESCE(q.square_sales, 0) as total_sales
FROM shopify s
FULL OUTER JOIN square q ON s.date = q.date
ORDER BY 1 DESC;

-- Daily Marketing Performance
CREATE OR REPLACE TABLE marts.marketing_performance AS
WITH ads AS (
    SELECT 
        date, 
        SUM(cost) as ad_cost,
        SUM(clicks) as ad_clicks,
        SUM(impressions) as ad_impressions,
        SUM(conversions) as ad_conversions,
        SUM(conversions_value) as ad_attributed_sales
    FROM core.ads_campaign
    GROUP BY 1
),
ga4 AS (
    SELECT 
        date, 
        SUM(sessions) as sessions,
        SUM(users) as users,
        SUM(revenue) as ga4_revenue
    FROM core.ga4_daily
    GROUP BY 1
),
sales AS (
    SELECT date, total_sales FROM marts.daily_revenue
)
SELECT
    COALESCE(a.date, g.date, s.date) as date,
    COALESCE(a.ad_cost, 0) as ad_cost,
    COALESCE(a.ad_clicks, 0) as ad_clicks,
    COALESCE(a.ad_impressions, 0) as ad_impressions,
    COALESCE(a.ad_conversions, 0) as ad_conversions,
    COALESCE(a.ad_attributed_sales, 0) as ad_attributed_sales,
    COALESCE(g.sessions, 0) as sessions,
    COALESCE(g.users, 0) as users,
    COALESCE(s.total_sales, 0) as total_sales,
    CASE WHEN COALESCE(a.ad_cost, 0) > 0 THEN COALESCE(a.ad_attributed_sales, 0) / a.ad_cost ELSE 0 END as roas
FROM ads a
FULL OUTER JOIN ga4 g ON COALESCE(a.date, DATE '1970-01-01') = g.date
FULL OUTER JOIN sales s ON COALESCE(a.date, g.date) = s.date
ORDER BY 1 DESC;

-- Product Sales (Shopify + Square Combined)
CREATE OR REPLACE TABLE marts.product_sales AS
WITH shopify_products AS (
    SELECT 
        COALESCE(m.unified_name, s.title) as product_name,
        SUM(s.qty) as quantity,
        SUM(s.total_price) as revenue,
        'Shopify' as source
    FROM core.shopify_orders s
    LEFT JOIN seeds.product_mapping m ON s.title = m.source_name
    GROUP BY 1
),
square_products AS (
    SELECT 
        COALESCE(m.unified_name, s.product_name) as product_name,
        SUM(s.quantity) as quantity,
        SUM(s.total_price) as revenue,
        'Square' as source
    FROM core.square_orders s
    LEFT JOIN seeds.product_mapping m ON s.product_name = m.source_name
    GROUP BY 1
),
combined AS (
    SELECT * FROM shopify_products
    UNION ALL
    SELECT * FROM square_products
)
SELECT 
    product_name,
    SUM(quantity) as total_quantity,
    SUM(revenue) as total_revenue,
    SUM(CASE WHEN source = 'Shopify' THEN revenue ELSE 0 END) as shopify_revenue,
    SUM(CASE WHEN source = 'Square' THEN revenue ELSE 0 END) as square_revenue
FROM combined
GROUP BY product_name
ORDER BY total_revenue DESC;
