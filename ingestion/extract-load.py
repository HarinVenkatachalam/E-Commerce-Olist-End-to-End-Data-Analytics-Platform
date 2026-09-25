import os
import sys
import logging
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from datetime import datetime

from logging.handlers import RotatingFileHandler
 
# ============================================================================

# PRODUCTION LOGGING CONFIGURATION (Dual Target: Console + Rotating File Log)

# ============================================================================

logger = logging.getLogger("OlistPipelineEngine")

logger.setLevel(logging.INFO)
 
# Define unified formatting layout

log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
 
# Target 1: Local Terminal Window (Console stdout)

console_handler = logging.StreamHandler(sys.stdout)

console_handler.setFormatter(log_format)

logger.addHandler(console_handler)
 
# Target 2: Rotating Persistent Local Log File

os.makedirs("logs", exist_ok=True)

file_handler = RotatingFileHandler(

    filename="logs/pipeline_execution.log", 

    maxBytes=5 * 1024 * 1024,  # Automatically rotates once the file hits 5 MegaBytes

    backupCount=3,             # Keeps a historical timeline backlog of up to 3 old files before deleting

    encoding="utf-8"

)

file_handler.setFormatter(log_format)

logger.addHandler(file_handler)
 
 
# Expected Structural Contract Schema to catch File drift or Column omissions
EXPECTED_METADATA_CONTRACT = {
    'olist_orders_dataset.csv': ('ORDER_RAW', ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']),
    'olist_order_items_dataset.csv': ('ORDER_ITEM_RAW', ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value']),
    'olist_order_payments_dataset.csv': ('ORDER_PAYMENT_RAW', ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']),
    'olist_order_reviews_dataset.csv': ('ORDER_REVIEW_RAW', ['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp']),
    'olist_customers_dataset.csv': ('CUSTOMER_RAW', ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']),
    'olist_sellers_dataset.csv': ('SELLER_RAW', ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']),
    'olist_products_dataset.csv': ('PRODUCT_RAW', ['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']),
    'olist_geolocation_dataset.csv': ('GEOLOCATION_RAW', ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 'geolocation_city', 'geolocation_state']),
    'product_category_name_translation.csv': ('PRODUCT_CATEGORY_TRANSLATION_RAW', ['product_category_name', 'product_category_name_english'])
}

def send_sms_incident_alert(message_body: str):

    """

    Triggers an emergency programmatic text alert using Twilio API

    to notify the on-call engineer of pipeline deployment failures.

    """

    try:

        from twilio.rest import Client

        # Pull API tokens from secure hidden environment settings

        account_sid = os.environ.get("TWILIO_ACCOUNT_SID")

        auth_token = os.environ.get("TWILIO_AUTH_TOKEN")

        from_number = os.environ.get("TWILIO_FROM_NUMBER")

        to_number = os.environ.get("TWILIO_TO_NUMBER")

        if not all([account_sid, auth_token, from_number, to_number]):

            logging.warning("⚠️ Twilio text alerts skipped: Environment variables incomplete.")

            return
 
        client = Client(account_sid, auth_token)

        message = client.messages.create(

            body=f"🚨 PIPELINE FAILURE ALERT: {message_body[:140]}",

            from_=from_number,

            to=to_number

        )

        logging.info(f"📲 Incident SMS sent successfully. Message SID: {message.sid}")

    except Exception as sms_error:

        logging.error(f"❌ Failed to dispatch emergency text alerting. Context: {sms_error}")
 


def download_from_kaggle(target_dir):
    """
    Uses the official Kaggle API client to pull down the Olist 
    e-commerce relational dataset bundle dynamically at runtime.
    """
    logger.info("📥 Step 1: Initiating Kaggle API Dataset Extraction...")
    try:
        import kaggle
    except Exception as e:
        logger.error(e)
        logger.critical("❌ Error: Failed to import Kaggle client. Ensure 'kaggle' is installed in your virtual environment.")
        sys.exit(1)
    os.makedirs(target_dir, exist_ok=True)
    dataset_slug = "olistbr/brazilian-ecommerce"
    try:
        logger.info(f"⚡ Downloading and unzipping {dataset_slug} from Kaggle API...")
        kaggle.api.dataset_download_files(dataset_slug, path=target_dir, unzip=True)
        logger.info("✅ Success: Dataset fully downloaded and unzipped locally.")
    except Exception as api_error:
        logger.error(api_error)
        logger.critical(f"❌ Kaggle API Failure: Authentication or download crashed. Details: {api_error}")
        logger.critical("💡 Verification Check: Ensure your kaggle.json token is stored in your user ~/.kaggle/ folder.")
        sys.exit(1)

def get_snowflake_connection():

    """

    Establishes an enterprise-grade connection to Snowflake utilizing 

    Asymmetric Key-Pair Authentication instead of cleartext passwords.

    """

    private_key_path = os.environ.get("SF_PRIVATE_KEY_PATH")

    passphrase = os.environ.get("SF_PRIVATE_KEY_PASSPHRASE")
    print ("key path" + private_key_path)
    if not private_key_path or not passphrase:

        logger.info("❌ Error: Missing private key configurations in environment setup.")

        sys.exit(1)

    try:

        # 1. Read and decrypt your local private key file block

        with open(private_key_path, "rb") as key_file:

            p_key = serialization.load_pem_private_key(

                key_file.read(),

                password=passphrase.encode(),

                backend=default_backend()

            )

        # 2. Convert the decrypted key asset into the DER byte array required by the Snowflake driver

        pkb = p_key.private_bytes(

            encoding=serialization.Encoding.DER,

            format=serialization.PrivateFormat.PKCS8,

            encryption_algorithm=serialization.NoEncryption()

        )

        # 3. Establish the secure connection block passing the private key bytes

        conn = snowflake.connector.connect(

            user=os.environ.get("SF_USER"),

            account=os.environ.get("SF_ACCOUNT"),

            warehouse=os.environ.get("SF_WAREHOUSE", "LOAD_WH"),

            database=os.environ.get("SF_DATABASE", "OLIST_DB"),

            schema="BRONZE",

            private_key=pkb  # Injects the cryptographic byte array signature

        )

        return conn

    except Exception as e:
        logger.error(e)

        logger.critical(f"❌ Connection Error: Key Pair Authentication failed. Details: {e}")

        raise

 
 
 
def run_atomic_elt_pipeline():

    logger.info(f"🏁 Starting Enterprise Olist Processing Engine at {datetime.now()}")

    DATA_DIR = os.environ.get("OLIST_DATA_DIR", "./data/raw/")

    download_from_kaggle(DATA_DIR)

    # -------------------------------------------------------------------------

    # STEP 2: VERIFY FILES AND SCHEMA COLUMN CONTRACTS

    # -------------------------------------------------------------------------

    logger.info("🔎 Step 2: Validating physical files and header schemas...")

    staged_dataframes = {}
 
    for file_name, (target_table, expected_columns) in EXPECTED_METADATA_CONTRACT.items():

        file_path = os.path.join(DATA_DIR, file_name)

        # Assert file presence and file sizing properties

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:

            error_msg = f"Critical Error: Missing or empty data file asset detected: {file_name}"

            logger.error(f"❌ {error_msg}")

            #send_sms_incident_alert(error_msg)

            sys.exit(1)

        # Extract row samples to verify structural columns match expectations

        df_sample = pd.read_csv(file_path, nrows=1)

        actual_columns = [col.strip().lower() for col in df_sample.columns]

        if set(expected_columns) != set(actual_columns):

            error_msg = f"Schema Contract Drift on {file_name}. Expected: {expected_columns}, Found: {actual_columns}"

            logger.error(f"❌ {error_msg}")

            #send_sms_incident_alert(error_msg)

            sys.exit(1)

        # Load verified file assets fully into memory as permissive VARCHAR text structures

        df_full = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig')

        df_full.columns = [col.strip().upper() for col in df_full.columns]

        df_full['LOADED_AT'] = datetime.utcnow().isoformat()

        df_full['SOURCE_FILE'] = file_name

        staged_dataframes[target_table] = df_full

    logger.info("✅ All files and column schemas successfully passed data-governance validation gates.")
 
    # -------------------------------------------------------------------------

    # STEP 3: ATOMIC MULTI-TABLE BATCH TRANSACTION COMMIT / ROLLBACK

    # -------------------------------------------------------------------------

    logger.info("🔌 Step 3: Initiating database transactions block...")

    conn = get_snowflake_connection()

    cursor = conn.cursor()

    try:

        # Enforce an explicit transactional context wrapper boundary

        cursor.execute("BEGIN TRANSACTION;")

        for table_name, df_payload in staged_dataframes.items():

            # Build programmatic dynamic schemas natively inside the database boundary

            ddl_columns = ", ".join([f"{col} VARCHAR" for col in df_payload.columns])

            cursor.execute(f"CREATE TABLE IF NOT EXISTS BRONZE.{table_name} ({ddl_columns});")

            # Execute data frame stream pushdown

            success, nchunks, nrows, _ = write_pandas(

                conn=conn, df=df_payload, table_name=table_name, schema="BRONZE", quote_identifiers=False

            )

            if not success:

                raise Exception(f"Bulk data write execution confirmation mismatch logged on table: {table_name}")

            logger.info(f"   ↳ Safely staged {nrows} rows into streaming grid: BRONZE.{table_name}")

        # If all tables load cleanly, commit changes to Bronze simultaneously

        cursor.execute("COMMIT;")

        logger.info("🎉 SUCCESS: Multi-table bulk load transaction committed. All 9 tables locked into BRONZE layer.")

    except Exception as pipeline_error:

        # Emergency retraction mechanism: Wipe the slate clean if any error is caught
        logger.error(pipeline_error)
        logger.critical(f"❌ CRITICAL RUNTIME ERROR: Loading loop broken. Initiating rollback sequence.")

        cursor.execute("ROLLBACK;")

        logger.error("🔄 Complete system rollback executed. All table changes discarded to prevent warehouse pollution.")

        error_msg = f"ELT Transaction Aborted. Database state rolled back. Reason: {pipeline_error}"

        #send_sms_incident_alert(error_msg)

    finally:

        cursor.close()

        conn.close()
 
def main ():
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    
    if not os.getenv("SF_USER") or not os.getenv("SF_PRIVATE_KEY_PATH"):
        logger.info("Error: Missing credentials. Please populate your hidden local .env file before running.")
    else:
        run_atomic_elt_pipeline()

 
    print("test1")
main()