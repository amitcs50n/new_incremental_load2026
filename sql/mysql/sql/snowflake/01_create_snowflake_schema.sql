-- =====================================================================
-- Snowflake Schema — Pharma Sales Pipeline
-- =====================================================================
-- Star schema target for the pipeline. Two schemas separate concerns:
--
--   RAW   — staging layer; mirrors MySQL exactly; OVERWRITTEN every run
--   CORE  — curated dim+fact layer; surrogate keys; SCD2 on customer;
--           PERSISTENT (never truncated)
--
-- Design notes:
--   - Surrogate keys (NUMBER IDENTITY) on every dim. Fact references
--     dims via surrogate keys, not natural keys. This decouples the
--     warehouse from source system identifier changes.
--   - SCD Type 2 on DIM_CUSTOMER only: customer_id is the natural key
--     (NOT unique here — multiple rows per customer_id, one per version).
--     effective_from / effective_to / is_current / scd_hash track history.
--   - SCD Type 1 on DIM_TERRITORY, DIM_PRODUCT, DIM_SALES_REP: natural
--     keys are UNIQUE; rows are upserted in place.
--   - FACT_SALES.transaction_id is UNIQUE — protects against accidental
--     duplicate inserts during MERGE.
--   - etl_loaded_at / etl_updated_at on every CORE table for traceability.
--
-- Run as SYSADMIN:
--   USE ROLE SYSADMIN;
--   -- then run this file
-- =====================================================================

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS PHARMA_DB;
USE DATABASE PHARMA_DB;

CREATE SCHEMA IF NOT EXISTS RAW;     -- staging (mirror of MySQL)
CREATE SCHEMA IF NOT EXISTS CORE;    -- curated dim + fact



-- =====================================================================
-- STAGING LAYER (RAW schema) — overwritten each run
-- =====================================================================
USE SCHEMA PHARMA_DB.RAW;

CREATE OR REPLACE TABLE STG_TERRITORIES (
    territory_id      NUMBER(38,0),
    territory_name    VARCHAR(100),
    region            VARCHAR(50),
    country           VARCHAR(50),
    created_at        TIMESTAMP_NTZ,
    updated_at        TIMESTAMP_NTZ,
    is_deleted        BOOLEAN
);

CREATE OR REPLACE TABLE STG_PRODUCTS (
    product_id        NUMBER(38,0),
    product_name      VARCHAR(200),
    category          VARCHAR(50),
    manufacturer      VARCHAR(100),
    unit_price        NUMBER(10,2),
    created_at        TIMESTAMP_NTZ,
    updated_at        TIMESTAMP_NTZ,
    is_deleted        BOOLEAN
);

CREATE OR REPLACE TABLE STG_SALES_REPS (
    rep_id            NUMBER(38,0),
    rep_name          VARCHAR(150),
    email             VARCHAR(150),
    territory_id      NUMBER(38,0),
    hire_date         DATE,
    created_at        TIMESTAMP_NTZ,
    updated_at        TIMESTAMP_NTZ,
    is_deleted        BOOLEAN
);

CREATE OR REPLACE TABLE STG_CUSTOMERS (
    customer_id       NUMBER(38,0),
    customer_name     VARCHAR(200),
    customer_type     VARCHAR(20),
    territory_id      NUMBER(38,0),
    address           VARCHAR(255),
    city              VARCHAR(100),
    state             VARCHAR(100),
    created_at        TIMESTAMP_NTZ,
    updated_at        TIMESTAMP_NTZ,
    is_deleted        BOOLEAN
);

CREATE OR REPLACE TABLE STG_SALES_TRANSACTIONS (
    transaction_id    NUMBER(38,0),
    product_id        NUMBER(38,0),
    customer_id       NUMBER(38,0),
    rep_id            NUMBER(38,0),
    transaction_date  DATE,
    quantity          NUMBER(38,0),
    unit_price        NUMBER(10,2),
    total_amount      NUMBER(12,2),
    created_at        TIMESTAMP_NTZ,
    updated_at        TIMESTAMP_NTZ,
    is_deleted        BOOLEAN
);


-- =====================================================================
-- CORE LAYER — surrogate keys, SCD2 on customer, persistent
-- =====================================================================
USE SCHEMA PHARMA_DB.CORE;

CREATE OR REPLACE TABLE DIM_TERRITORY (
    territory_sk      NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    territory_id      NUMBER(38,0) NOT NULL,
    territory_name    VARCHAR(100),
    region            VARCHAR(50),
    country           VARCHAR(50),
    is_deleted        BOOLEAN     DEFAULT FALSE,
    etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uk_dim_territory UNIQUE (territory_id)
);

CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_sk        NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    product_id        NUMBER(38,0) NOT NULL,
    product_name      VARCHAR(200),
    category          VARCHAR(50),
    manufacturer      VARCHAR(100),
    unit_price        NUMBER(10,2),
    is_deleted        BOOLEAN     DEFAULT FALSE,
    etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uk_dim_product UNIQUE (product_id)
);

CREATE OR REPLACE TABLE DIM_SALES_REP (
    rep_sk            NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    rep_id            NUMBER(38,0) NOT NULL,
    rep_name          VARCHAR(150),
    email             VARCHAR(150),
    territory_id      NUMBER(38,0),
    hire_date         DATE,
    is_deleted        BOOLEAN     DEFAULT FALSE,
    etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uk_dim_sales_rep UNIQUE (rep_id)
);

-- SCD Type 2 — multiple rows per customer_id, one per period of validity
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_sk       NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    customer_id       NUMBER(38,0) NOT NULL,        -- natural key (NOT unique here)
    customer_name     VARCHAR(200),
    customer_type     VARCHAR(20),
    territory_id      NUMBER(38,0),
    address           VARCHAR(255),
    city              VARCHAR(100),
    state             VARCHAR(100),
    is_deleted        BOOLEAN     DEFAULT FALSE,

    -- SCD Type 2 columns
    effective_from    TIMESTAMP_NTZ NOT NULL,
    effective_to      TIMESTAMP_NTZ,                -- NULL = currently active
    is_current        BOOLEAN       NOT NULL DEFAULT TRUE,
    scd_hash          VARCHAR(64),                  -- hash of tracked attrs (change detection)

    etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE FACT_SALES (
    sales_sk          NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    transaction_id    NUMBER(38,0) NOT NULL,

    -- Surrogate FKs to dim versions active at sale time
    product_sk        NUMBER(38,0),
    customer_sk       NUMBER(38,0),                 -- locks in customer-as-they-were
    rep_sk            NUMBER(38,0),
    territory_sk      NUMBER(38,0),

    -- Measures
    transaction_date  DATE         NOT NULL,
    quantity          NUMBER(38,0),
    unit_price        NUMBER(10,2),
    total_amount      NUMBER(12,2),

    is_deleted        BOOLEAN      DEFAULT FALSE,
    etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uk_fact_sales UNIQUE (transaction_id)
);
