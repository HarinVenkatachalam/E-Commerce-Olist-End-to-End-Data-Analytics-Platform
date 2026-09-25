USE DATABASE OLIST_DB;

USE SCHEMA SILVER;
 
CREATE OR REPLACE PROCEDURE SILVER.PRC_TRANSFORM_SILVER_BATCH()

RETURNS VARCHAR

LANGUAGE SQL

EXECUTE AS CALLER

AS

$$

DECLARE

    v_geol_dropped INT;
	
	v_geol_outofbounds_dropped INT;

    v_order_dropped INT;

	v_order_deliverybeforepurchase_dropped INT;
	
    v_item_dropped INT;
	
	v_item_negative_dropped INT;
	
	v_customer_dropped INT;

	v_seller_dropped INT;
	
	v_product_dropped INT;
	
	v_payment_dropped INT; 
	
	v_review_dropped INT;
	
    v_orphan_count INT;

BEGIN

    -- 1. ESTABLISH THE ATOMIC TRANSACTION BOUNDARY

    BEGIN TRANSACTION;
 
    -- ========================================================================

    -- INCREMENTAL MERGE (Driven by Change-Data-Capture Streams)

    -- ========================================================================

-- Inbound Audit: Trace missing primary key glitches in the current stream batch
    SELECT COUNT(*) INTO :v_geol_dropped

    FROM BRONZE.STR_GEOLOCATION_RAW

    WHERE GEOLOCATION_ZIP_CODE_PREFIX IS NULL;
	
	 
-- Inbound Audit: Trace out of bound lat and long glitches in the current stream batch
	SELECT COUNT(*) INTO :v_geol_outofbounds_dropped

    FROM BRONZE.STR_GEOLOCATION_RAW

    WHERE TRY_CAST(GEOLOCATION_LAT AS FLOAT) NOT BETWEEN -33.8 AND 5.3

       OR TRY_CAST(GEOLOCATION_LNG AS FLOAT) NOT BETWEEN -74.0 AND -34.7;
	   
 
    MERGE INTO SILVER.GEOLOCATION_STG target

    USING(
        SELECT 
    
            TRIM(GEOLOCATION_ZIP_CODE_PREFIX) AS ZIP_CODE_PREFIX,
    
            AVG(TRY_CAST(GEOLOCATION_LAT AS FLOAT)) AS LATITUDE,
    
            AVG(TRY_CAST(GEOLOCATION_LNG AS FLOAT)) AS LONGITUDE,
    
            MAX(TRIM(UPPER(REGEXP_REPLACE(GEOLOCATION_CITY, '[^a-zA-Z0-9 ÁÉÍÓÚÂÊÔÀãõçñÁéíóúâêôàãõçñ]', '')))) AS CITY,
    
            MAX(TRIM(UPPER(GEOLOCATION_STATE))) AS STATE,
    
            CURRENT_TIMESTAMP() AS PROCESSED_AT
    
        FROM BRONZE.STR_GEOLOCATION_RAW
    
        WHERE GEOLOCATION_ZIP_CODE_PREFIX IS NOT NULL 
		
		  AND TRY_CAST(GEOLOCATION_LAT AS FLOAT) BETWEEN -33.8 AND 5.3
    
          AND TRY_CAST(GEOLOCATION_LNG AS FLOAT) BETWEEN -74.0 AND -34.7
    
        GROUP BY 1
    ) source
    
    ON target.ZIP_CODE_PREFIX = source.ZIP_CODE_PREFIX
    WHEN MATCHED THEN 

        UPDATE SET 

            target.LATITUDE = source.LATITUDE,

            target.LONGITUDE = source.LONGITUDE,

            target.CITY = source.CITY,

            target.STATE = source.STATE,

            target.PROCESSED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN 

        INSERT (ZIP_CODE_PREFIX, LATITUDE, LONGITUDE, CITY, STATE)

        VALUES (source.ZIP_CODE_PREFIX, source.LATITUDE, source.LONGITUDE, source.CITY, source.STATE);
 
 


-- Inbound Audit: Trace missing primary key glitches in the current stream batch
	 SELECT COUNT(*) INTO :v_order_dropped

    FROM BRONZE.STR_ORDER_RAW

    WHERE ORDER_ID IS NULL;

-- Inbound Audit: Trace chronological time-travel glitches in the current stream batch
    SELECT COUNT(*) INTO :v_order_deliverybeforepurchase_dropped

    FROM BRONZE.STR_ORDER_RAW

    WHERE TRY_TO_TIMESTAMP(ORDER_DELIVERED_CUSTOMER_DATE) < TRY_TO_TIMESTAMP(ORDER_PURCHASE_TIMESTAMP);
	
 
    MERGE INTO SILVER.ORDER_STG target 

    USING (

        SELECT 

            TRIM(LOWER(ORDER_ID)) AS ORDER_ID,

            TRIM(LOWER(CUSTOMER_ID)) AS CUSTOMER_ID,

            TRIM(UPPER(ORDER_STATUS)) AS ORDER_STATUS,

            TRY_TO_TIMESTAMP(ORDER_PURCHASE_TIMESTAMP) AS PURCHASE_TS,

            TRY_TO_TIMESTAMP(ORDER_APPROVED_AT) AS APPROVED_TS,

            TRY_TO_TIMESTAMP(ORDER_DELIVERED_CARRIER_DATE) AS CARRIER_TS,

            TRY_TO_TIMESTAMP(ORDER_DELIVERED_CUSTOMER_DATE) AS DELIVERED_TS,

            TRY_TO_TIMESTAMP(ORDER_ESTIMATED_DELIVERY_DATE) AS ESTIMATED_TS

        FROM BRONZE.STR_ORDER_RAW -- Consuming incrementally from the Bronze CDC Stream

        WHERE ORDER_ID IS NOT NULL

          AND (TRY_TO_TIMESTAMP(ORDER_DELIVERED_CUSTOMER_DATE) >= TRY_TO_TIMESTAMP(ORDER_PURCHASE_TIMESTAMP) 

               OR ORDER_DELIVERED_CUSTOMER_DATE IS NULL)

        QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY TRY_TO_TIMESTAMP(ORDER_PURCHASE_TIMESTAMP) DESC) = 1

    ) source 

    ON target.ORDER_ID = source.ORDER_ID

    WHEN MATCHED THEN 

        UPDATE SET 

            target.ORDER_STATUS = source.ORDER_STATUS,

            target.ORDER_DELIVERED_CUSTOMER_DATE = source.DELIVERED_TS,

            target.PROCESSED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN 

        INSERT (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_PURCHASE_TIMESTAMP, ORDER_APPROVED_AT, ORDER_DELIVERED_CARRIER_DATE, ORDER_DELIVERED_CUSTOMER_DATE, ORDER_ESTIMATED_DELIVERY_DATE, IS_CHRONOLOGICALLY_VALID)

        VALUES (source.ORDER_ID, source.CUSTOMER_ID, source.ORDER_STATUS, source.PURCHASE_TS, source.APPROVED_TS, source.CARRIER_TS, source.DELIVERED_TS, source.ESTIMATED_TS, TRUE);
 
 
    -- Incremental Merge for Order Items
	
-- Inbound Audit: Trace missing primary key glitches in the current stream batch
    SELECT COUNT(*) INTO :v_item_dropped

    FROM BRONZE.STR_ORDER_ITEM_RAW

    WHERE ORDER_ID IS  NULL OR ORDER_ITEM_ID IS  NULL ;



-- Inbound Audit: Trace -ve prive or freight glitches in the current stream batch

    SELECT COUNT(*) INTO :v_item_negative_dropped

    FROM BRONZE.STR_ORDER_ITEM_RAW

    WHERE TRY_CAST(PRICE AS NUMBER(10,2)) < 0 OR TRY_CAST(FREIGHT_VALUE AS NUMBER(10,2)) < 0;
	
 
    MERGE INTO SILVER.ORDER_ITEM_STG target 

    USING (

        SELECT 

            TRIM(LOWER(ORDER_ID)) AS ORDER_ID,

            TRY_CAST(ORDER_ITEM_ID AS INT) AS ORDER_ITEM_ID,

            TRIM(LOWER(PRODUCT_ID)) AS PRODUCT_ID,

            TRIM(LOWER(SELLER_ID)) AS SELLER_ID,

            CASE WHEN TRY_CAST(PRICE AS NUMBER(10,2)) < 0 THEN 0.00 ELSE TRY_CAST(PRICE AS NUMBER(10,2)) END AS PRICE,

            CASE WHEN TRY_CAST(FREIGHT_VALUE AS NUMBER(10,2)) < 0 THEN 0.00 ELSE TRY_CAST(FREIGHT_VALUE AS NUMBER(10,2)) END AS FREIGHT_VALUE

        FROM BRONZE.STR_ORDER_ITEM_RAW

        WHERE ORDER_ID IS NOT NULL and ORDER_ITEM_ID IS NOT NULL 
        
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID, ORDER_ITEM_ID ORDER BY LOADED_AT DESC) = 1

    ) source 

    ON target.ORDER_ID = source.ORDER_ID AND target.ORDER_ITEM_ID = source.ORDER_ITEM_ID

    WHEN MATCHED THEN 

        UPDATE SET 

            target.PRODUCT_ID = source.PRODUCT_ID,

            target.SELLER_ID = source.SELLER_ID,

            target.PRICE = source.PRICE,
            
            target.FREIGHT_VALUE = source.FREIGHT_VALUE,
            
            target.PROCESSED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN 

        INSERT (ORDER_ID, ORDER_ITEM_ID, PRODUCT_ID, SELLER_ID, PRICE, FREIGHT_VALUE)

        VALUES (source.ORDER_ID, source.ORDER_ITEM_ID, source.PRODUCT_ID, source.SELLER_ID, source.PRICE, source.FREIGHT_VALUE);


    -- Incremental Merge for CUSTOMER
	
	-- Inbound Audit: Trace missing primary key glitches in the current stream batch

    SELECT COUNT(*) INTO :v_customer_dropped

    FROM BRONZE.STR_CUSTOMER_RAW

    WHERE CUSTOMER_ID IS NULL;
	
 
    MERGE INTO SILVER.CUSTOMER_STG target 

    USING (

        SELECT 
    	TRIM(LOWER(CUSTOMER_ID)) AS CUSTOMER_ID,
    	TRIM(LOWER(CUSTOMER_UNIQUE_ID)) AS CUSTOMER_UNIQUE_ID,
   	    TRIM(CUSTOMER_ZIP_CODE_PREFIX) AS ZIP_CODE_PREFIX,
    	TRIM(UPPER(CUSTOMER_CITY)) AS CUSTOMER_CITY,
    	TRIM(UPPER(CUSTOMER_STATE)) AS CUSTOMER_STATE,
    	CURRENT_TIMESTAMP()
        FROM BRONZE.STR_CUSTOMER_RAW
        WHERE CUSTOMER_ID is not NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY LOADED_AT DESC) = 1

    ) source 

    ON target.CUSTOMER_ID = source.CUSTOMER_ID

    WHEN MATCHED THEN 

        UPDATE SET 

            target.ZIP_CODE_PREFIX = source.ZIP_CODE_PREFIX,

            target.CUSTOMER_UNIQUE_ID = source.CUSTOMER_UNIQUE_ID,
            
            target.CITY = source.CUSTOMER_CITY,

            target.STATE = source.CUSTOMER_STATE,
            
            target.PROCESSED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN 

        INSERT (CUSTOMER_ID, CUSTOMER_UNIQUE_ID, ZIP_CODE_PREFIX, CITY, STATE)

        VALUES (SOURCE.CUSTOMER_ID, SOURCE.CUSTOMER_UNIQUE_ID, SOURCE.ZIP_CODE_PREFIX, SOURCE.CUSTOMER_CITY, SOURCE.CUSTOMER_STATE);

-- Incremental Merge for SELLER

	-- Inbound Audit: Trace missing primary key glitches in the current stream batch

    SELECT COUNT(*) INTO :v_seller_dropped

    FROM BRONZE.STR_SELLER_RAW

    WHERE SELLER_ID IS NULL;
 
    MERGE INTO SILVER.SELLER_STG target 

    USING (

        SELECT
	    TRIM(LOWER(SELLER_ID)) AS SELLER_ID,
    	TRIM(SELLER_ZIP_CODE_PREFIX) AS ZIP_CODE_PREFIX,
   	    TRIM(UPPER(SELLER_CITY)) AS CITY,
    	TRIM(UPPER(SELLER_STATE)) AS STATE,
    	CURRENT_TIMESTAMP()
        FROM BRONZE.STR_SELLER_RAW
        WHERE SELLER_ID IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SELLER_ID ORDER BY LOADED_AT DESC) = 1

    ) source 

    ON target.SELLER_ID = source.SELLER_ID
    
    WHEN MATCHED THEN 

        UPDATE SET 

            target.ZIP_CODE_PREFIX = source.ZIP_CODE_PREFIX,

            target.CITY = source.CITY,

            target.STATE = source.STATE,
            
            target.PROCESSED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN 

        INSERT (SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE)

        VALUES (SOURCE.SELLER_ID, SOURCE.ZIP_CODE_PREFIX, SOURCE.CITY, SOURCE.STATE);

 
 -- Incremental Merge for PRODUCT
	
	-- Inbound Audit: Trace missing primary key glitches in the current stream batch

    SELECT COUNT(*) INTO :v_product_dropped

    FROM BRONZE.STR_PRODUCT_RAW

    WHERE PRODUCT_ID IS NULL;
	
 
    MERGE INTO SILVER.PRODUCT_STG target 

    USING (

        SELECT 

			TRIM(LOWER(PRODUCT_ID)) AS PRODUCT_ID,

			COALESCE(TRIM(UPPER(PCT.PRODUCT_CATEGORY_NAME_ENGLISH)), 'UNASSIGNED') AS CATEGORY_NAME_ENGLISH,

			TRY_CAST(p.PRODUCT_WEIGHT_G AS INT) AS PRODUCT_WEIGHT_G,

			TRY_CAST(p.PRODUCT_LENGTH_CM AS INT) AS PRODUCT_LENGTH_CM,

			TRY_CAST(p.PRODUCT_HEIGHT_CM AS INT) AS PRODUCT_HEIGHT_CM,

			TRY_CAST(p.PRODUCT_WIDTH_CM AS INT) AS PRODUCT_WIDTH_CM,

			CURRENT_TIMESTAMP()

        FROM BRONZE.STR_PRODUCT_RAW P left join 

		(SELECT 

		TRIM(UPPER(REGEXP_REPLACE(PRODUCT_CATEGORY_NAME, '[^a-zA-Z0-9 ÁÉÍÓÚÂÊÔÀãõçñÁéíóúâêôàãõçñ]', ''))) AS PRODUCT_CATEGORY_NAME , 

		MAX(TRIM(UPPER(REGEXP_REPLACE(PRODUCT_CATEGORY_NAME_ENGLISH, '[^a-zA-Z0-9 ÁÉÍÓÚÂÊÔÀãõçñÁéíóúâêôàãõçñ]', '')))) AS PRODUCT_CATEGORY_NAME_ENGLISH

		from BRONZE.PRODUCT_CATEGORY_TRANSLATION_RAW 

		group by TRIM(UPPER(REGEXP_REPLACE(PRODUCT_CATEGORY_NAME, '[^a-zA-Z0-9 ÁÉÍÓÚÂÊÔÀãõçñÁéíóúâêôàãõçñ]', ''))) 

		)

		as PCT on TRIM(UPPER(REGEXP_REPLACE(P.PRODUCT_CATEGORY_NAME, '[^a-zA-Z0-9 ÁÉÍÓÚÂÊÔÀãõçñÁéíóúâêôàãõçñ]', '')))

		= PCT.PRODUCT_CATEGORY_NAME

		WHERE P.PRODUCT_ID IS NOT NULL
 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY LOADED_AT DESC) = 1

    ) source 

    ON target.PRODUCT_ID = source.PRODUCT_ID
    
    WHEN MATCHED THEN 

        UPDATE SET 

            target.CATEGORY_NAME_ENGLISH = source.CATEGORY_NAME_ENGLISH,
            target.PRODUCT_WEIGHT_G = source.PRODUCT_WEIGHT_G,
            target.PRODUCT_LENGTH_CM = source.PRODUCT_LENGTH_CM,
            target.PRODUCT_HEIGHT_CM = source.PRODUCT_HEIGHT_CM,
            target.PRODUCT_WIDTH_CM = source.PRODUCT_WIDTH_CM,
            target.PROCESSED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN 

        INSERT (PRODUCT_ID, CATEGORY_NAME_ENGLISH, PRODUCT_WEIGHT_G, PRODUCT_LENGTH_CM, PRODUCT_HEIGHT_CM, PRODUCT_WIDTH_CM)

        VALUES (SOURCE.PRODUCT_ID, SOURCE.CATEGORY_NAME_ENGLISH, SOURCE.PRODUCT_WEIGHT_G, SOURCE.PRODUCT_LENGTH_CM, SOURCE.PRODUCT_HEIGHT_CM, SOURCE.PRODUCT_WIDTH_CM);

 -- Incremental Merge for PAYMENT

	-- Inbound Audit: Trace missing primary key glitches in the current stream batch

    SELECT COUNT(*) INTO :v_payment_dropped

    FROM BRONZE.STR_ORDER_PAYMENT_RAW

    WHERE ORDER_ID IS NULL OR  PAYMENT_SEQUENTIAL IS  NULL;
 
 
    MERGE INTO SILVER.PAYMENT_STG target 

    USING (

        SELECT
	    TRIM(LOWER(ORDER_ID)) AS ORDER_ID,
        TRY_CAST(PAYMENT_SEQUENTIAL AS INT) AS PAYMENT_SEQUENTIAL,
        TRIM(UPPER(PAYMENT_TYPE)) AS PAYMENT_TYPE,
        TRY_CAST(PAYMENT_INSTALLMENTS AS INT) AS PAYMENT_INSTALLMENTS,
        TRY_CAST(PAYMENT_VALUE AS NUMBER(10,2)) AS PAYMENT_VALUE,
    	CURRENT_TIMESTAMP()
        FROM BRONZE.STR_ORDER_PAYMENT_RAW
        WHERE ORDER_ID IS NOT NULL AND PAYMENT_SEQUENTIAL IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID,PAYMENT_SEQUENTIAL ORDER BY LOADED_AT DESC) = 1

    ) source 

    ON target.ORDER_ID = source.ORDER_ID AND target.PAYMENT_SEQUENTIAL = source.PAYMENT_SEQUENTIAL
    
    WHEN MATCHED THEN 

        UPDATE SET 

            target.PAYMENT_TYPE = source.PAYMENT_TYPE,
            target.PAYMENT_INSTALLMENTS = source.PAYMENT_INSTALLMENTS,
            target.PAYMENT_VALUE = source.PAYMENT_VALUE,
            
            target.PROCESSED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN 

        INSERT (ORDER_ID,PAYMENT_SEQUENTIAL,PAYMENT_TYPE,PAYMENT_INSTALLMENTS,PAYMENT_VALUE)

        VALUES (source.ORDER_ID,source.PAYMENT_SEQUENTIAL,source.PAYMENT_TYPE,source.PAYMENT_INSTALLMENTS,source.PAYMENT_VALUE);

 -- Incremental Merge for REVIEW

    SELECT COUNT(*) INTO :v_review_dropped

    FROM BRONZE.STR_ORDER_REVIEW_RAW

    WHERE REVIEW_ID IS NULL OR ORDER_ID IS NULL;
 
    MERGE INTO SILVER.REVIEW_STG target 

    USING (

        SELECT
	    TRIM(LOWER(REVIEW_ID)) AS REVIEW_ID,
		TRIM(LOWER(ORDER_ID)) AS ORDER_ID,
		TRY_CAST(REVIEW_SCORE AS INT) AS REVIEW_SCORE,
		TRY_TO_TIMESTAMP(REVIEW_CREATION_DATE) AS REVIEW_CREATION_DATE,
		TRY_TO_TIMESTAMP(REVIEW_ANSWER_TIMESTAMP) AS REVIEW_ANSWER_TIMESTAMP,
    	CURRENT_TIMESTAMP()
        FROM BRONZE.STR_ORDER_REVIEW_RAW
        WHERE REVIEW_ID IS NOT NULL AND ORDER_ID IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY REVIEW_ID,ORDER_ID ORDER BY LOADED_AT DESC,TRY_TO_TIMESTAMP(REVIEW_ANSWER_TIMESTAMP) DESC,TRY_TO_TIMESTAMP(REVIEW_CREATION_DATE) DESC) = 1

    ) source 

    ON  target.REVIEW_ID = source.REVIEW_ID AND target.ORDER_ID = source.ORDER_ID 
    
    WHEN MATCHED THEN 

        UPDATE SET 

            target.REVIEW_SCORE = source.REVIEW_SCORE,
            target.REVIEW_CREATION_DATE = source.REVIEW_CREATION_DATE,
            target.REVIEW_ANSWER_TIMESTAMP = source.REVIEW_ANSWER_TIMESTAMP,
            target.PROCESSED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN 

        INSERT (REVIEW_ID,ORDER_ID,REVIEW_SCORE,REVIEW_CREATION_DATE,REVIEW_ANSWER_TIMESTAMP)

        VALUES (source.REVIEW_ID,source.ORDER_ID,source.REVIEW_SCORE,source.REVIEW_CREATION_DATE,source.REVIEW_ANSWER_TIMESTAMP);

 
 
    -- ========================================================================

    -- [POST-MERGE AUDIT] DATA QUALITY TELEMETRY REPORTING

    -- ========================================================================

    SELECT COUNT(*) INTO :v_orphan_count

    FROM SILVER.ORDER_ITEM_STG oi

    LEFT JOIN SILVER.PRODUCT_STG p ON oi.PRODUCT_ID = p.PRODUCT_ID

    WHERE p.PRODUCT_ID IS NULL;
 
    INSERT INTO SILVER.DQ_HEALTH_AUDIT_LOG (METRIC_CATEGORY, SOURCE_ENTITY, ANOMALY_DESCRIPTION, RECORDS_AFFECTED_COUNT)

    VALUES 

		('PRIMARY_KEY_CONSTRAINT', 'GEOLOCATION_RAW', 'EXCULDED : Incremental items add with missing zip_code_prefix primary key', :v_geol_dropped),
		
        ('RANGE_BOUNDS_CONSTRAINT', 'GEOLOCATION_RAW', 'EXCULDED : Coordinates dropped outside bounding box of Brazil', :v_geol_outofbounds_dropped),

		('PRIMARY_KEY_CONSTRAINT', 'STR_ORDER_RAW', 'EXCULDED : Incremental items add with missing order_id primary key', :v_order_dropped),

        ('CHRONOLOGY_VIOLATION', 'STR_ORDER_RAW', 'EXCULDED : Incremental orders dropped due to delivery date anomalies', :v_order_deliverybeforepurchase_dropped),

		('PRIMARY_KEY_CONSTRAINT', 'STR_ORDER_ITEM_RAW', 'EXCULDED : Incremental items add with missing order_id or  order_item_id primary key', :v_item_dropped),
		
        ('FORMAT_VALUE_CONSTRAINT', 'STR_ORDER_ITEM_RAW', 'WARNING: Incremental items modified due to negative prices', :v_item_negative_dropped),
		
		('PRIMARY_KEY_CONSTRAINT', 'STR_CUSTOMER_RAW', 'EXCULDED : Incremental items add with missing customer_id primary key', :v_customer_dropped),
		 
		('PRIMARY_KEY_CONSTRAINT', 'STR_SELLER_RAW', 'EXCULDED : Incremental items add with missing seller_id primary key', :v_seller_dropped),

		('PRIMARY_KEY_CONSTRAINT', 'STR_PRODUCT_RAW', 'EXCULDED : Incremental items add with missing product_id primary key', :v_product_dropped),
	
		('PRIMARY_KEY_CONSTRAINT', 'STR_ORDER_PAYMENT_RAW', 'EXCULDED : Incremental items add with missing order_id or payment_sequential primary key', :v_payment_dropped),
		
		('PRIMARY_KEY_CONSTRAINT', 'STR_ORDER_REVIEW_RAW', 'EXCULDED : Incremental items add with missing review_id primary key', :v_review_dropped),

        ('REFERENTIAL_INTEGRITY', 'ORDER_ITEM', 'WARNING: Orphan item rows referencing missing product_id records', :v_orphan_count);
 
    -- 2. SECURE THE ENTIRE PLATFORM TRANSACTION STATE

    COMMIT;

    RETURN ' SUCCESS: LOOKUPS OVERWRITTEN / TRANSACTIONS INCREMENTALLY MERGED SUCCESSFULLY.';
 
EXCEPTION

    WHEN OTHER THEN

        -- 3. THE ATOMIC EMERGENCY RETRACTION

        ROLLBACK;

        RETURN ' CRITICAL: Pipeline transaction aborted. Complete database rollback executed. Trace: ' || SQLERRM;

END;

$$;

 