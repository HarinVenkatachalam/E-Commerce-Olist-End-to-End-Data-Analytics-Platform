use database OLIST_DB;
use schema BRONZE;

--Customers Raw Landing Table
create or replace table CUSTOMER_RAW(
    CUSTOMER_ID varchar,
    CUSTOMER_UNIQUE_ID varchar,
    CUSTOMER_ZIP_CODE_PREFIX varchar,
    CUSTOMER_CITY varchar,
    CUSTOMER_STATE varchar,
    -- Enterprise Audit Metadata
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Geolocation Raw Landing Table
create or replace table GEOLOCATION_RAW(
    GEOLOCATION_ZIP_CODE_PREFIX varchar,
    GEOLOCATION_LAT varchar,
    GEOLOCATION_LNG varchar,
    GEOLOCATION_CITY varchar,
    GEOLOCATION_STATE varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Product Category Translation Lookup Table
create or replace table PRODUCT_CATEGORY_TRANSLATION_RAW(
    PRODUCT_CATEGORY_NAME varchar,
    PRODUCT_CATEGORY_NAME_ENGLISH varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Products Raw Landing Table
create or replace table PRODUCT_RAW(
    PRODUCT_ID varchar,
    PRODUCT_CATEGORY_NAME varchar,
    PRODUCT_NAME_LENGHT varchar,
    PRODUCT_DESCRIPTION_LENGHT varchar,
    PRODUCT_PHOTOS_QTY varchar,
    PRODUCT_WEIGHT_G varchar,
    PRODUCT_LENGTH_CM varchar,
    PRODUCT_HEIGHT_CM varchar,
    PRODUCT_WIDTH_CM varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Sellers Raw Landing Table
create or replace table SELLER_RAW(
    SELLER_ID varchar,
    SELLER_ZIP_CODE_PREFIX varchar,
    SELLER_CITY varchar,
    SELLER_STATE varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Orders Raw Landing Table
create or replace table ORDER_RAW(
    ORDER_ID varchar,
    CUSTOMER_ID varchar,
    ORDER_STATUS varchar,
    ORDER_PURCHASE_TIMESTAMP varchar,
    ORDER_APPROVED_AT varchar,
    ORDER_DELIVERED_CARRIER_DATE varchar,
    ORDER_DELIVERED_CUSTOMER_DATE varchar,
    ORDER_ESTIMATED_DELIVERY_DATE varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Order Items Raw Landing Table
create or replace table ORDER_ITEM_RAW(
    ORDER_ID varchar,
    ORDER_ITEM_ID varchar,
    PRODUCT_ID varchar,
    SELLER_ID varchar,
    SHIPPING_LIMIT_DATE varchar,
    PRICE varchar,
    FREIGHT_VALUE varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Order Payments Raw Landing Table
create or replace table ORDER_PAYMENT_RAW(
    ORDER_ID varchar,
    PAYMENT_SEQUENTIAL varchar,
    PAYMENT_TYPE varchar,
    PAYMENT_INSTALLMENTS varchar,
    PAYMENT_VALUE varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);
--Order Reviews Raw Landing Table
create or replace table ORDER_REVIEW_RAW(
    REVIEW_ID varchar,
    ORDER_ID varchar,
    REVIEW_SCORE varchar,
    REVIEW_COMMENT_TITLE varchar,
    REVIEW_COMMENT_MESSAGE varchar,
    REVIEW_CREATION_DATE varchar,
    REVIEW_ANSWER_TIMESTAMP varchar,
    LOADED_AT timestamp_ltz,
    SOURCE_FILE varchar
);

USE DATABASE OLIST_DB;
USE SCHEMA BRONZE;
 
CREATE OR REPLACE STREAM BRONZE.STR_ORDER_RAW ON TABLE BRONZE.ORDER_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_ORDER_ITEM_RAW ON TABLE BRONZE.ORDER_ITEM_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_GEOLOCATION_RAW ON TABLE BRONZE.GEOLOCATION_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_CUSTOMER_RAW ON TABLE BRONZE.CUSTOMER_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_PRODUCT_CATEGORY_TRANSLATION_RAW ON TABLE BRONZE.PRODUCT_CATEGORY_TRANSLATION_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_PRODUCT_RAW ON TABLE BRONZE.PRODUCT_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_SELLER_RAW ON TABLE BRONZE.SELLER_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_ORDER_PAYMENT_RAW ON TABLE BRONZE.ORDER_PAYMENT_RAW;
CREATE OR REPLACE STREAM BRONZE.STR_ORDER_REVIEW_RAW ON TABLE BRONZE.ORDER_REVIEW_RAW;
