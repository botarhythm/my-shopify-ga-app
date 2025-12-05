-- Initialize Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS seeds;

-- Raw Tables (Staging)
-- These tables will be truncated and reloaded or appended to by the ETL process

-- Seeds
CREATE TABLE IF NOT EXISTS seeds.product_mapping (
    source_name VARCHAR,
    unified_name VARCHAR
);

-- Shopify
CREATE TABLE IF NOT EXISTS raw.shopify_orders (
    date DATE,
    order_id VARCHAR,
    lineitem_id VARCHAR,
    product_id VARCHAR,
    variant_id VARCHAR,
    sku VARCHAR,
    title VARCHAR,
    qty BIGINT,
    price DOUBLE,
    order_total DOUBLE,
    created_at TIMESTAMP,
    currency VARCHAR,
    total_price DOUBLE,
    financial_status VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Square
CREATE TABLE IF NOT EXISTS raw.square_payments (
    payment_id VARCHAR,
    created_at TIMESTAMP,
    date DATE,
    amount DOUBLE,
    currency VARCHAR,
    status VARCHAR,
    order_id VARCHAR,
    source_type VARCHAR,
    card_brand VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Square Orders (Line Items / Products)
CREATE TABLE IF NOT EXISTS raw.square_orders (
    order_id VARCHAR,
    created_at TIMESTAMP,
    date DATE,
    product_name VARCHAR,
    quantity DOUBLE,
    base_price DOUBLE,
    total_price DOUBLE,
    currency VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GA4
CREATE TABLE IF NOT EXISTS raw.ga4_daily (
    date DATE,
    source VARCHAR,
    medium VARCHAR,
    campaign VARCHAR,
    sessions BIGINT,
    users BIGINT,
    revenue DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Google Ads
CREATE TABLE IF NOT EXISTS raw.ads_campaign (
    date DATE,
    campaign_id VARCHAR,
    campaign_name VARCHAR,
    cost DOUBLE,
    clicks BIGINT,
    impressions BIGINT,
    conversions DOUBLE,
    conversions_value DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
