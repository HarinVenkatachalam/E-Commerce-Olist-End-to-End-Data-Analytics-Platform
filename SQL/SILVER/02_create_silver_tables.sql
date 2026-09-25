USE DATABASE OLIST_DB;

USE SCHEMA SILVER;
 
-- ============================================================================

-- 1. GEOLOCATION CENTROIDS (Build First as a Lookup Matrix)

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.GEOLOCATION_STG (

    ZIP_CODE_PREFIX VARCHAR(15) PRIMARY KEY,

    LATITUDE FLOAT,

    LONGITUDE FLOAT,

    CITY VARCHAR(100),

    STATE VARCHAR(10),

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);
 
 
-- ============================================================================

-- 2. CUSTOMERS STAGING

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.CUSTOMER_STG (

    CUSTOMER_ID VARCHAR(32) PRIMARY KEY,

    CUSTOMER_UNIQUE_ID VARCHAR(32),

    ZIP_CODE_PREFIX VARCHAR(15),

    CITY VARCHAR(100),

    STATE VARCHAR(10),

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);
 
 
-- ============================================================================

-- 3. SELLERS STAGING

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.SELLER_STG (

    SELLER_ID VARCHAR(32) PRIMARY KEY,

    ZIP_CODE_PREFIX VARCHAR(15),

    CITY VARCHAR(100),

    STATE VARCHAR(10),

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);

 
-- ============================================================================

-- 4. PRODUCTS & LANGUAGE TRANSLATION STAGING

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.PRODUCT_STG (

    PRODUCT_ID VARCHAR(32) PRIMARY KEY,

    CATEGORY_NAME_ENGLISH VARCHAR(100),

    PRODUCT_WEIGHT_G INT,

    PRODUCT_LENGTH_CM INT,

    PRODUCT_HEIGHT_CM INT,

    PRODUCT_WIDTH_CM INT,

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);
 
 
-- ============================================================================

-- 5. ORDERS STAGING (With Chronology Verification)

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.ORDER_STG (

    ORDER_ID VARCHAR(32) PRIMARY KEY,

    CUSTOMER_ID VARCHAR(32),

    ORDER_STATUS VARCHAR(20),

    ORDER_PURCHASE_TIMESTAMP TIMESTAMP_NTZ,

    ORDER_APPROVED_AT TIMESTAMP_NTZ,

    ORDER_DELIVERED_CARRIER_DATE TIMESTAMP_NTZ,

    ORDER_DELIVERED_CUSTOMER_DATE TIMESTAMP_NTZ,

    ORDER_ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,

    IS_CHRONOLOGICALLY_VALID BOOLEAN,

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);
 
 
-- ============================================================================

-- 6. ORDER ITEMS STAGING

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.ORDER_ITEM_STG (

    ORDER_ID VARCHAR(32),

    ORDER_ITEM_ID INT,

    PRODUCT_ID VARCHAR(32),

    SELLER_ID VARCHAR(32),

    PRICE NUMBER(10,2),

    FREIGHT_VALUE NUMBER(10,2),

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (ORDER_ID, ORDER_ITEM_ID)

);
 
 
-- ============================================================================

-- 7. ORDER PAYMENTS STAGING

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.PAYMENT_STG (

    ORDER_ID VARCHAR(32),

    PAYMENT_SEQUENTIAL INT,

    PAYMENT_TYPE VARCHAR(20),

    PAYMENT_INSTALLMENTS INT,

    PAYMENT_VALUE NUMBER(10,2),

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);
 
 
-- ============================================================================

-- 8. ORDER REVIEWS STAGING

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.REVIEW_STG (

    REVIEW_ID VARCHAR(32),

    ORDER_ID VARCHAR(32),

    REVIEW_SCORE INT,

    REVIEW_CREATION_DATE TIMESTAMP_NTZ,

    REVIEW_ANSWER_TIMESTAMP TIMESTAMP_NTZ,

    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);
 
 
-- ============================================================================

-- 9. REFERENTIAL DATA QUALITY AUDIT LOG

-- ============================================================================

CREATE OR REPLACE TABLE SILVER.DQ_REFERENTIAL_LOG (

    SOURCE_TABLE VARCHAR(50),

    TARGET_TABLE VARCHAR(50),

    VIOLATING_KEY VARCHAR(50),

    ORPHAN_COUNT INT,

    LOGGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

);

 CREATE TABLE IF NOT EXISTS SILVER.DQ_HEALTH_AUDIT_LOG (

        AUDIT_ID INT AUTOINCREMENT,

        METRIC_CATEGORY VARCHAR(50),

        SOURCE_ENTITY VARCHAR(50),

        ANOMALY_DESCRIPTION VARCHAR(255),

        RECORDS_AFFECTED_COUNT INT,

        LOGGED_AT_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()

    );
 
