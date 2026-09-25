============================================================================
-- LEVEL 1: ADMINISTRATIVE WAREHOUSE & DATABASE SETUP-- 
============================================================================
use role accountadmin;

-- Create dedicated virtual warehouses separating ingestion workloads from transformation workloads
CREATE OR REPLACE WAREHOUSE LOAD_WH
     WITH WAREHOUSE_SIZE = 'XSMALL'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     COMMENT = 'Dedicated compute for Python bulk data loading';
CREATE OR REPLACE WAREHOUSE TRANSFORM_WH
     WITH WAREHOUSE_SIZE = 'XSMALL'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     COMMENT = 'Dedicated compute for automated Streams and Tasks DAG execution';
-- Create the primary database platform
CREATE DATABASE IF NOT EXISTS OLIST_DB;
USE DATABASE OLIST_DB; -- Create the Medallion Architecture data layers
CREATE SCHEMA IF NOT EXISTS BRONZE; -- Landed raw text staging zone
CREATE SCHEMA IF NOT EXISTS SILVER; -- Cleaned, typed, deduplicated entity models
CREATE SCHEMA IF NOT EXISTS GOLD; -- Star schema dimension, facts, and aggregated business marts

--==========================================================================
--LEVEL 2: SECURITY & ROLE-BASED ACCESS CONTROL (RBAC)-- ============================================================================
-- Create a dedicated transformation engineer role to show enterprise security awareness
CREATE ROLE IF NOT EXISTS TRANSFORMER_ROLE;

-- Grant object permissions to your transformation role
GRANT USAGE, OPERATE ON WAREHOUSE LOAD_WH TO ROLE TRANSFORMER_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER_ROLE;
GRANT ALL PRIVILEGES ON DATABASE OLIST_DB TO ROLE TRANSFORMER_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE OLIST_DB TO ROLE TRANSFORMER_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE OLIST_DB TO ROLE TRANSFORMER_ROLE;




-- ============================================================================
-- Script: Create Key-Pair Authentication User (No Network Policy)
-- Role Required: ACCOUNTADMIN 
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE OLIST_DB;

DROP USER INGESTION_SERVICE_USER;
 
CREATE OR REPLACE USER INGESTION_SERVICE_USER
    TYPE = LEGACY_SERVICE 
    RSA_PUBLIC_KEY = '<your RSA public key>'
    DEFAULT_ROLE = TRANSFORMER_ROLE
    DEFAULT_WAREHOUSE = LOAD_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Asymmetric key-pair user account for automated Python ingestion (No Firewall Network Constraint)';
 
-- ----------------------------------------------------------------------------
-- STEP 2: AUTHORIZE PRIVILEGES & VERIFY
-- ----------------------------------------------------------------------------
-- Map the ingestion user to your transformation tracking role matrix
GRANT ROLE TRANSFORMER_ROLE TO USER INGESTION_SERVICE_USER;
 
-- Execute the description command below to confirm the public key is active.
DESCRIBE USER INGESTION_SERVICE_USER;

