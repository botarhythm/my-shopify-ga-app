-- Transform Raw to Core

-- Core Shopify Orders
-- Deduplicate based on order_id and lineitem_id
CREATE OR REPLACE TABLE core.shopify_orders AS
SELECT DISTINCT
    date,
    order_id,
    lineitem_id,
    product_id,
    variant_id,
    sku,
    title,
    qty,
    price,
    order_total,
    created_at,
    currency,
    total_price,
    financial_status
FROM raw.shopify_orders;

-- Core Square Payments
-- Deduplicate based on payment_id
CREATE OR REPLACE TABLE core.square_payments AS
SELECT DISTINCT
    payment_id,
    created_at,
    date,
    amount,
    currency,
    status,
    order_id,
    source_type,
    card_brand
FROM raw.square_payments;

-- Core Square Orders (Product Line Items)
CREATE OR REPLACE TABLE core.square_orders AS
SELECT DISTINCT
    order_id,
    created_at,
    date,
    product_name,
    quantity,
    base_price,
    total_price,
    currency
FROM raw.square_orders;

-- Core GA4
CREATE OR REPLACE TABLE core.ga4_daily AS
SELECT DISTINCT
    date,
    source,
    medium,
    campaign,
    sessions,
    users,
    revenue
FROM raw.ga4_daily;

-- Core Ads
CREATE OR REPLACE TABLE core.ads_campaign AS
SELECT DISTINCT
    date,
    campaign_id,
    campaign_name,
    cost,
    clicks,
    impressions,
    conversions,
    conversions_value
FROM raw.ads_campaign;
