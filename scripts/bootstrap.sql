-- scripts/bootstrap.sql

-- ★依存順：stg_* → core_* → mart_* → mart_daily_yoy

-- core
CREATE TABLE IF NOT EXISTS core_ga4 AS
SELECT * FROM (SELECT NULL::DATE AS date, NULL::VARCHAR AS source, NULL::VARCHAR AS channel,
                      NULL::VARCHAR AS page_path, 0::BIGINT AS sessions, 0::BIGINT AS users,
                      0::BIGINT AS purchases, 0.0::DOUBLE AS revenue)
WHERE 1=0;

CREATE TABLE IF NOT EXISTS core_shopify AS
SELECT * FROM (SELECT NULL::DATE AS date, NULL::UBIGINT AS order_id, NULL::UBIGINT AS lineitem_id,
                      NULL::UBIGINT AS product_id, NULL::UBIGINT AS variant_id, NULL::VARCHAR AS sku,
                      NULL::VARCHAR AS title, 0::BIGINT AS qty, 0.0::DOUBLE AS price, 0.0::DOUBLE AS order_total,
                      NULL::VARCHAR AS created_at, NULL::VARCHAR AS currency, 0.0::DOUBLE AS total_price,
                      0.0::DOUBLE AS total_discounts, 0::BIGINT AS shipping_lines, 0::BIGINT AS tax_lines)
WHERE 1=0;

CREATE TABLE IF NOT EXISTS core_square AS
SELECT * FROM (SELECT NULL::DATE AS date, NULL::VARCHAR AS payment_id, NULL::VARCHAR AS created_at, 0.0::DOUBLE AS amount,
                      NULL::VARCHAR AS currency, NULL::VARCHAR AS card_brand, NULL::VARCHAR AS status,
                      NULL::VARCHAR AS receipt_number, NULL::VARCHAR AS order_id, NULL::VARCHAR AS location_id,
                      NULL::VARCHAR AS merchant_id, NULL::VARCHAR AS card_type, NULL::VARCHAR AS card_fingerprint,
                      NULL::VARCHAR AS entry_method, 0.0::DOUBLE AS processing_fee)
WHERE 1=0;

CREATE TABLE IF NOT EXISTS core_ads_campaign AS
SELECT * FROM (SELECT NULL::DATE AS date, NULL::UBIGINT AS campaign_id, NULL::VARCHAR AS campaign_name,
                      0.0::DOUBLE AS cost, 0::BIGINT AS clicks, 0::BIGINT AS impressions,
                      0.0::DOUBLE AS conversions, 0.0::DOUBLE AS conv_value)
WHERE 1=0;

-- marts（ビューでOK。必要なら後でマテリアライズ）
CREATE OR REPLACE VIEW mart_revenue_daily AS
WITH s AS (SELECT date, SUM(order_total) AS shop_rev FROM core_shopify GROUP BY 1),
     q AS (SELECT date, SUM(amount) AS sq_rev   FROM core_square  GROUP BY 1),
     d AS (
       SELECT date FROM core_ga4
       UNION SELECT date FROM core_shopify
       UNION SELECT date FROM core_square
     )
SELECT d.date,
       COALESCE(s.shop_rev,0)+COALESCE(q.sq_rev,0) AS total_revenue,
       COALESCE(s.shop_rev,0) AS shopify_revenue,
       COALESCE(q.sq_rev,0) AS square_revenue
FROM d
LEFT JOIN s USING(date)
LEFT JOIN q USING(date);

CREATE OR REPLACE VIEW mart_traffic_daily AS
SELECT date,
       SUM(sessions)  AS sessions,
       SUM(users)     AS users,
       SUM(purchases) AS purchases,
       SUM(revenue)   AS ga_revenue
FROM core_ga4 GROUP BY 1;

CREATE OR REPLACE VIEW mart_ads_daily AS
SELECT date,
       SUM(cost)        AS cost,
       SUM(clicks)      AS clicks,
       SUM(impressions) AS impressions,
       SUM(conversions) AS conversions,
       SUM(conv_value)  AS conv_value
FROM core_ads_campaign GROUP BY 1;

CREATE OR REPLACE VIEW mart_daily AS
SELECT d.date,
       t.sessions, t.users, t.purchases, t.ga_revenue,
       r.total_revenue, r.shopify_revenue, r.square_revenue,
       a.cost, a.clicks, a.impressions, a.conversions, a.conv_value,
       CASE WHEN a.cost>0 THEN (r.total_revenue/a.cost) END AS roas
FROM (SELECT DISTINCT date FROM mart_revenue_daily
      UNION SELECT DISTINCT date FROM mart_traffic_daily
      UNION SELECT DISTINCT date FROM mart_ads_daily) d
LEFT JOIN mart_traffic_daily t USING(date)
LEFT JOIN mart_revenue_daily r USING(date)
LEFT JOIN mart_ads_daily a USING(date);

-- ★YoYはビューで管理：無い問題を根本解消
CREATE OR REPLACE VIEW mart_daily_yoy AS
WITH prev AS (
  SELECT (date + INTERVAL '1 year') AS date,
         sessions AS sessions_prev,
         total_revenue AS total_revenue_prev,
         roas AS roas_prev
  FROM mart_daily
)
SELECT c.date,
       c.sessions, p.sessions_prev,
       c.total_revenue, p.total_revenue_prev,
       c.roas, p.roas_prev
FROM mart_daily c
LEFT JOIN prev p USING(date);
