-- =====================================================
-- STORED PROCEDURE: sp_load_silver_layer
-- Purpose: Load and clean data from Bronze to Silver layer
-- Execution Order:
--   1. crm_cust_info
--   2. crm_prd_info
--   3. crm_sales_details
--   4. erp_cust_az12
--   5. erp_loc_a101
--   6. erp_px_cat_g1v2
-- =====================================================

CREATE OR REPLACE PROCEDURE silver.sp_load_silver_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    
    -- Initialize logging
    v_start_time := NOW();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting Silver Layer Load Process at %', v_start_time;
    RAISE NOTICE '========================================';
    
    -- =====================================================
    -- 1. LOAD CRM_CUST_INFO
    -- Cleaning: Trim firstname/lastname, normalize marital status and gender
    -- =====================================================
    RAISE NOTICE '';
    RAISE NOTICE '>>> STEP 1: Loading CRM_CUST_INFO...';
    
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        -- CASE FOR THE MARITAL STATUS
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        -- CASE FOR THE GENDER
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
    ) t 
    WHERE flag_last = 1;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'CRM_CUST_INFO: % rows inserted', v_rows_affected;
    
    -- =====================================================
    -- 2. LOAD CRM_PRD_INFO
    -- Cleaning: Extract category ID and product key, normalize product line, 
    -- calculate end dates based on next start date
    -- =====================================================
    RAISE NOTICE '';
    RAISE NOTICE '>>> STEP 2: Loading CRM_PRD_INFO...';
    
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract Category ID
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Extract Product key
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        -- CASE FOR THE PRD_LINE
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line, -- Map product line code to descriptive values
        CAST(prd_start_dt AS DATE),
        CAST(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - INTERVAL '1 day' AS DATE
        ) AS prd_end_dt -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'CRM_PRD_INFO: % rows inserted', v_rows_affected;
    
    -- =====================================================
    -- 3. LOAD CRM_SALES_DETAILS
    -- Cleaning: Fix sales and price values with validation logic
    -- =====================================================
    RAISE NOTICE '';
    RAISE NOTICE '>>> STEP 3: Loading CRM_SALES_DETAILS...';
    
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        -- Fix sales value
        CASE 
            WHEN sls_sales IS NULL 
                 OR sls_sales <= 0 
                 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- Fix price value
        CASE 
            WHEN sls_price IS NULL 
                 OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'CRM_SALES_DETAILS: % rows inserted', v_rows_affected;
    
    -- =====================================================
    -- 4. LOAD ERP_CUST_AZ12
    -- Cleaning: Remove 'NAS' prefix from cid, validate birthdates, normalize gender
    -- =====================================================
    RAISE NOTICE '';
    RAISE NOTICE '>>> STEP 4: Loading ERP_CUST_AZ12...';
    
    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' Prefix if present
            ELSE cid
        END AS cid,
        CASE WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate, -- Set future birthdates to NULL
        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen -- Normalize gender value and handle unknown cases
    FROM bronze.erp_cust_az12;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'ERP_CUST_AZ12: % rows inserted', v_rows_affected;
    
    -- =====================================================
    -- 5. LOAD ERP_LOC_A101
    -- Cleaning: Remove hyphens from cid, normalize country codes
    -- =====================================================
    RAISE NOTICE '';
    RAISE NOTICE '>>> STEP 5: Loading ERP_LOC_A101...';
    
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry -- Normalize and handle missing or blank country codes
    FROM bronze.erp_loc_a101;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'ERP_LOC_A101: % rows inserted', v_rows_affected;
    
    -- =====================================================
    -- 6. LOAD ERP_PX_CAT_G1V2
    -- Cleaning: Direct load (minimal transformation required)
    -- =====================================================
    RAISE NOTICE '';
    RAISE NOTICE '>>> STEP 6: Loading ERP_PX_CAT_G1V2...';
    
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'ERP_PX_CAT_G1V2: % rows inserted', v_rows_affected;
    
    -- =====================================================
    -- COMPLETION LOG
    -- =====================================================
    v_end_time := NOW();
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Silver Layer Load Process Completed';
    RAISE NOTICE 'Start Time: %', v_start_time;
    RAISE NOTICE 'End Time: %', v_end_time;
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '========================================';
    
    -- Commit the transaction
    COMMIT;
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error in sp_load_silver_layer: % %', SQLSTATE, SQLERRM;
    ROLLBACK;
    
END;
$$;

-- =====================================================
-- PROCEDURE EXECUTION
-- =====================================================
-- To execute the stored procedure, use:
-- CALL public.sp_load_silver_layer();

-- =====================================================
-- OPTIONAL: Create a monitoring table for execution logs
-- =====================================================
/*
CREATE TABLE IF NOT EXISTS public.sp_load_silver_layer_logs (
    log_id SERIAL PRIMARY KEY,
    execution_start_time TIMESTAMP NOT NULL,
    execution_end_time TIMESTAMP,
    duration_seconds NUMERIC,
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Modified procedure version with logging to table:
-- (Replace the completion section with the one below)

INSERT INTO public.sp_load_silver_layer_logs (
    execution_start_time, 
    execution_end_time, 
    duration_seconds, 
    status
) VALUES (
    v_start_time, 
    v_end_time, 
    EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
    'SUCCESS'
);

*/
