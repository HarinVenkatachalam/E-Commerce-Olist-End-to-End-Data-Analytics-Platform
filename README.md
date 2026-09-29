# E-Commerce Olist End-to-End Data Analytics Platform: Modern Data Stack ELT & Data Analytics 

## 📌Project Overview 
An enterprise-grade, cloud-native **ELT Data Platform** transforming **100K+ highly fragmented, multi-grain transactional records** from the Olist Brazilian E-Commerce ecosystem into a production-ready **Dimensional Star Schema**. 
This platform shifts processing logic completely away from brittle local environments to **Snowflake Cloud Pushdown Compute**, orchestrating the entire lifecycle through a secure, passwordless **Asymmetric RSA Key-Pair Ingestion Layer**, a unified **Transactional Stored Procedure**, and an executive **Power BI Command Center**.
---

## 🏗️ 1. End-to-End System Architecture
 
```text
                        [KAGGLE API ENDPOINT]
                                  │
          (Dynamic Runtime Extraction via Python Kaggle API Client)
                                  ▼
                       [LOCAL RUNTIME ENGINE]
                                  │
(Asymmetric RSA Key-Pair Auth + Encrypted JWT Private Key Signature Exchange)
                                  ▼
                   [SNOWFLAKE CLOUD DATA PLATFORM]
                                  │
      ┌───────────────────────────┴───────────────────────────┐
      ▼                                                       ▼
[BRONZE LANDING ZONE]                               [TECHNICAL MONITORING]
  ↳ Permissive Strings Schema (VARCHAR)               ↳ Rotating Local File Logs
  ↳ Ingestion Lineage (Metadata Anchors)              ↳ Twilio API SMS Alerting
      │
      ▼ (Scheduled Serverless Snowflake Task Trigger / Stream CDC Gate)
[SILVER STAGING ZONE]
  ↳ Atomic Transaction Block (`BEGIN TRANSACTION` + `ROLLBACK` Intercepts)
  ↳ Global Geospatial Centroid Averaging & Boundary Bounding Mask
  ↳ String Accent Parsing via `TRIM(UPPER(REGEXP_REPLACE()))` & Type Casting
  ↳ Data Quality Gate Tracking & Attrition Logging (`SILVER.DQ_HEALTH_AUDIT_LOG`)
      │
      ▼ (Deterministic MD5-Hashed Surrogate Keys)
[GOLD STAR SCHEMA LAYER]
  ↳ Multi-Grain Fact Tables Separation (`fact_order_line`, `fact_payment`, `fact_review`)
  ↳ Dimensional Isolation (`dim_customer`, `dim_product`, `dim_geography`, `dim_date`)
  ↳ Defensive NULL Trajectory Evaluation & Orphan Key Containment Placement
      │
      ▼ (Import Mode / Secure Native DB Cloud Connection)
[POWER BI EXECUTIVE COMMAND CENTER]
  ↳ Repeat Purchase Engines, Process Bottleneck Funnels, & Freight Cost/KM Curves
```
## 🎯 2. Business Value & Core Analytical Inquiries
Operational leadership across multi-seller e-commerce marketplaces typically struggles with disconnected operational data silos, hidden system anomalies, and opaque carrier performance indicators. This platform resolves these structural friction points by delivering data to answer four critical executive queries:
1. **Fulfillment Compliance & Bottleneck funnels:** Where exactly are orders losing ground inside our internal processing pipeline? Are delays happening during warehouse picking, payment approvals, or transit handoffs?
2. **Freight Cost Elasticity vs. Supply Chain Physics:** Are we paying too much money for short-distance cargo moves? Which carrier networks break standard freight cost curves relative to physical travel?
3. **Shopper Retention Tipping Point:** What is the statistical threshold of delivery delay (SLA variance) that causes customer satisfaction scores to plummet? How badly does a poor first-order delivery experience damage long-term customer lifetime value?
4. **Platform Governance & Pipeline Observability:** How fresh is our data? What percentage of records are being dropped at our quality gates due to system clock anomalies or coordinates drift?
---
## ⚙️ 3. Engineering Design Decisions & Medallion Layer Evolution
### 🐍 Python Extraction & Ingestion Layer: Programmatic Decoupling
* **The Decision:** Replaced standard file uploads with an automated, runtime-driven API extraction suite inside `ingestion/extract_load.py`. This script pulls files dynamically via the Kaggle API, runs pre-load schema validation contracts, maps raw headers to a standardized singular layout, and transfers rows to Snowflake using passwordless Asymmetric RSA Key-Pair authentication.
* **The Rationale:** In an enterprise setting, manual browser downloads introduce human error, scale poorly, and create stale data assets. Decoupling the pipeline into an execution engine separates the platform's blueprint (code) from its database state.
#### Ingestion Engineering & Security Standards Implemented:
* **Asymmetric RSA Key-Pair Authentication:** Traditional single-factor passwords or static token strings present massive credential rotation vulnerabilities and risk exposure if pushed to public repositories. This pipeline leverages asymmetric cryptography: a local Private Key (`.p8`) file is read and decrypted locally at runtime using an environment-injected passphrase. The script passes a cryptographically signed signature block to the Snowflake driver, which Snowflake matches against the Public Key bound to the database profile.
* **Pre-Load Metadata Schema Contracts:** To ensure data quality before spinning up warehouse compute resources, the Python runtime environment acts as a structural gatekeeper. It holds a hardcoded structural schema contract (`EXPECTED_METADATA_CONTRACT`) mapping file names to explicit column signatures. If an upstream source changes an asset name, omits a column, or introduces schema drift, the script halts execution before data transfer occurs to protect warehouse stability.
* **Dual-Target Technical Monitoring & SMS Alerting:** For robust observability, the script uses a decoupled logging architecture. It routes runtime footprints to a local `RotatingFileHandler` (managing 5MB log caps with a 3-file backlog) while echoing outputs to the stdout console. If a transaction fails or a contract breach occurs, the script activates an emergency catch block that hooks into the Twilio API, texting the on-call data engineer directly.

### 🟫 Bronze Layer: Permissive Ingestion & Defensive Isolation
* **The Decision:** Incoming data ingested completely as **`VARCHAR`** data type into unique singular tables (`ORDER_RAW`, `CUSTOMER_RAW`). 
* **The Rationale:** In a production pipeline, if an upstream system pushes a corrupted string into a date field or alters column parameters, a strict layout will crash the ingestion thread. A permissive schema swallows the data raw, appending critical ingestion tracking fields (`LOADED_AT`, `SOURCE_FILE`), and delegates enforcement to the database engine where data can be managed without transaction dropouts.
### ⬜ Silver Layer: Transactional Consolidation & Data Harmonization
* **The Decision:** Collapsed the entire transformation engine into a single **Snowflake Stored Procedure** (`SILVER.PRC_TRANSFORM_SILVER_BATCH()`) executed inside an all-or-nothing transaction block (`BEGIN TRANSACTION ... COMMIT / ROLLBACK`), driven by an automated Snowflake Task.
* **The Rationale:** Parallel task DAG structures evaluate tables in isolation. If an ingestion load fails halfway through on a relational dataset like Olist, the environment falls into a fractured state (e.g., items load, but orders roll back), corrupting downstream reports. This architecture enforces **Cross-Table Atomicity**—if a single row violates a processing gate at 6 AM, the entire batch transaction aborts cleanly, maintaining zero warehouse pollution.
#### Silver Quality & Sanitization Standards Implemented:
* **Character Encoding & Accent Harmonization:** The source data maps real-world Brazilian marketplaces with Portuguese accents (`é`, `ã`, `ç`). Ingesting via basic drivers causes character corruption (e.g., `automã³vel`). The pipeline enforces a clean **`utf-8`** data tranformation locally to strip out hidden Windows Byte Order Marks (BOM), while running `TRIM(UPPER(REGEXP_REPLACE(...)))` in Snowflake to eliminate trailing strings and erase stray background symbols.
* **Geospatial Centroid Clustering:** The raw tracking dataset houses millions of conflicting coordinate logs for identical locations. Joining this directly introduces a devastating **fan-out multiplication bug**. The Silver procedure runs a bounding box mask limiting coordinates to Brazil's physical parameters (Lat: -33.8 to 5.3, Long: -74.0 to -34.7) and clusters rows via `AVG(latitude)`, compressing files into an optimized singular ZIP code lookup matrix.
* **Observability Attrition Logging:** Instead of silently deleting bad data, the procedure tracks dropped counts programmatically (e.g., rows where delivery timestamps precede purchase creation timestamps) and writes entries transactionally to a centralized metrics ledger (`SILVER.DQ_HEALTH_AUDIT_LOG`) to maintain complete transparency.
### 🟨 Gold Layer: Optimized Dimensional Star Schema View Layout
* **The Decision:** Modeled data views cleanly into a decoupled **Star Schema** using binary-deterministic **MD5-hashed surrogate keys** (`customer_key`, `product_key`). Fact boundaries are completely isolated at their specific natural grains (`gold.fact_order_line`, `gold.fact_payment`, `gold.fact_review`).
* **The Rationale:** Jamming installment tracking arrays or satisfaction scores into a single order lines table replicates severe many-to-many relationship fan-outs, causing Power BI calculation cards to multiply revenue aggregates artificially. 
#### Resolution of Architectural Traps:
* **The "Undelivered On-Time" Bug:** When evaluated naively (`delivered_date > estimated_date`), canceled, lost, or pending orders containing `NULL` arrival stamps evaluate to `NULL` (False), slipping into Power BI calculations as "On-Time" deliveries. The `gold.fact_order_line` architecture uses defensive conditional routing gates to explicitly classify `NULL` values or active cancellations as fulfillment failures.
* **The Transient Customer Key Glitch:** In Olist, `customer_id` is generated fresh at *every single checkout event*. Evaluating customer performance using this key indicates a repeat-purchase rate of exactly **0.0%**. The Gold view links the transactional row directly to the underlying permanent identifier (**`customer_unique_id`**), unlocking accurate cohort retention metrics.
---
## 🛠️ 4. Project Stuctre
E-Commerce-Olist-End-to-End-Platform/
│
├── data/
│   └── raw/
│       └── *.csv
│
├── sql/
│   ├── bronze/
│   ├── silver/
│   ├── setup/
│   └── gold/
│
├── ingestion/
│   ├── extract-load.py
│   └── requirements.txt
│
├── keys/
│   ├── snowflake_key.p8
│   └── snowflake_key
│
├── logs/
│
│
├── dashboards/
│   └── E-Commerce_OLIST_Executive_Command_Center.pbix
│
├── .env
├── .gitignore
├── LICENSE.txt
└── README.md

## 🚀 5. Infrastructure Deployment Manual
1. Initialize Snowflake Warehouses & Roles
Log into Snowflake as ACCOUNTADMIN, open a Worksheet, and execute the configuration statements found in your sql/bronze/01_initialize_warehouse_env.sql file.
2. Configure Local Environment Secrets (.env)
Create an isolated .env configuration matrix in your root directory. Ensure this file is explicitly listed in your .gitignore to protect production parameters:
```text
SF_USER=INGESTION_SERVICE_USER
SF_ACCOUNT=your_snowflake_locator_id (e.g., xy12345.us-east-1)
SF_WAREHOUSE=LOAD_WH
SF_DATABASE=OLIST_DB
SF_PRIVATE_KEY_PATH=./keys/snowflake_key.p8
SF_PRIVATE_KEY_PASSPHRASE=your_secret_private_key_passphrase
Optional Twilio Telemetry Credentials
TWILIO_ACCOUNT_SID=ACyour_sid
TWILIO_AUTH_TOKEN=your_token
TWILIO_FROM_NUMBER=+15551234567
TWILIO_TO_NUMBER=+15559876543
```
3. Deploy the Pipeline Execution
```bash
Initialize local Python environment packages
pip install -r requirements.txt
Export variables and execute the ingestion script
export $(cat .env | xargs)
python Ingestion/extract_load.py
```
 
***
