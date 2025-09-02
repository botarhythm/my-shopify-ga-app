-- src/transform/build_core.sql
-- コアテーブル構築（staging → core）

-- GA4 コアテーブル
CREATE TABLE IF NOT EXISTS core_ga4 AS
SELECT
    CAST(date AS DATE) AS date,
    COALESCE(source, '(not set)') AS source,
    sessionDefaultChannelGroup AS channel,
    pagePath AS page_path,
    CAST(sessions AS BIGINT) AS sessions,
    CAST(totalUsers AS BIGINT) AS users,
    CAST(purchases AS BIGINT) AS purchases,
    CAST(purchaseRevenue AS DOUBLE) AS revenue
FROM stg_ga4;

-- Shopify 注文コアテーブル
CREATE TABLE IF NOT EXISTS core_shopify_orders AS
SELECT
    CAST(strftime(created_at, '%Y-%m-%d') AS DATE) AS date,
    order_id,
    lineitem_id,
    product_id,
    variant_id,
    sku,
    title,
    CAST(quantity AS BIGINT) AS quantity,
    CAST(price AS DOUBLE) AS price,
    CAST(total_price AS DOUBLE) AS order_total,
    currency,
    CAST(total_discounts AS DOUBLE) AS total_discounts,
    CAST(shipping_lines AS BIGINT) AS shipping_lines,
    CAST(tax_lines AS BIGINT) AS tax_lines
FROM stg_shopify_orders;

-- Shopify 商品コアテーブル
CREATE TABLE IF NOT EXISTS core_shopify_products AS
SELECT
    product_id,
    product_title,
    product_handle,
    product_type,
    vendor,
    variant_id,
    variant_title,
    sku,
    CAST(price AS DOUBLE) AS price,
    CAST(compare_at_price AS DOUBLE) AS compare_at_price,
    CAST(inventory_quantity AS BIGINT) AS inventory_quantity,
    weight,
    weight_unit,
    created_at,
    updated_at
FROM stg_shopify_products;

-- Square 支払いコアテーブル
CREATE TABLE IF NOT EXISTS core_square_payments AS
SELECT
    CAST(created_at AS DATE) AS date,
    payment_id,
    CAST(amount AS DOUBLE) AS amount,
    currency,
    card_brand,
    status,
    receipt_number,
    order_id,
    location_id,
    merchant_id,
    card_type,
    card_fingerprint,
    entry_method,
    CAST(processing_fee AS DOUBLE) AS processing_fee
FROM stg_square_payments;

-- Google Ads キャンペーンコアテーブル
CREATE TABLE IF NOT EXISTS core_ads_campaign AS
SELECT
    CAST(date AS DATE) AS date,
    campaign_id,
    campaign_name,
    CAST(cost AS DOUBLE) AS cost,
    CAST(clicks AS BIGINT) AS clicks,
    CAST(impressions AS BIGINT) AS impressions,
    CAST(conversions AS DOUBLE) AS conversions,
    CAST(conversions_value AS DOUBLE) AS conversions_value
FROM stg_ads_campaign;

-- Google Ads 広告グループコアテーブル
CREATE TABLE IF NOT EXISTS core_ads_adgroup AS
SELECT
    CAST(date AS DATE) AS date,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    CAST(cost AS DOUBLE) AS cost,
    CAST(clicks AS BIGINT) AS clicks,
    CAST(impressions AS BIGINT) AS impressions,
    CAST(conversions AS DOUBLE) AS conversions,
    CAST(conversions_value AS DOUBLE) AS conversions_value
FROM stg_ads_adgroup;

-- Google Ads キーワードコアテーブル
CREATE TABLE IF NOT EXISTS core_ads_keyword AS
SELECT
    CAST(date AS DATE) AS date,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    keyword,
    CAST(cost AS DOUBLE) AS cost,
    CAST(clicks AS BIGINT) AS clicks,
    CAST(impressions AS BIGINT) AS impressions,
    CAST(conversions AS DOUBLE) AS conversions,
    CAST(conversions_value AS DOUBLE) AS conversions_value
FROM stg_ads_keyword;
