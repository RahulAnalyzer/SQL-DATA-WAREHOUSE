
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from CSV files to bronze tables.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();

PostgreSQL Notes:
    - Uses COPY command instead of SQL Server's BULK INSERT
    - CSV file paths must be accessible to the PostgreSQL server
    - The PostgreSQL service user must have read permissions on the CSV files
    - File paths use forward slashes or escaped backslashes
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
    v_duration_seconds NUMERIC;
    v_batch_duration_seconds NUMERIC;
BEGIN
    
    v_batch_start_time := NOW();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';
    
    -- ========================================================
    -- LOADING CRM TABLES
    -- ========================================================
    RAISE NOTICE '';
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';
    
    -- ========================================================
    -- 1. LOAD bronze.crm_cust_info
    -- ========================================================
    BEGIN
        v_start_time := NOW();
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info FROM '/path/to/datasets/source_crm/cust_info.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        
        v_end_time := NOW();
        v_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        RAISE NOTICE '>> Load Duration: % seconds', ROUND(v_duration_seconds, 2);
        RAISE NOTICE '>> -------';
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'ERROR loading crm_cust_info: % %', SQLSTATE, SQLERRM;
    END;
    
    -- ========================================================
    -- 2. LOAD bronze.crm_prd_info
    -- ========================================================
    BEGIN
        v_start_time := NOW();
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info FROM '/path/to/datasets/source_crm/prd_info.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        
        v_end_time := NOW();
        v_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        RAISE NOTICE '>> Load Duration: % seconds', ROUND(v_duration_seconds, 2);
        RAISE NOTICE '>> -------';
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'ERROR loading crm_prd_info: % %', SQLSTATE, SQLERRM;
    END;
    
    -- ========================================================
    -- 3. LOAD bronze.crm_sales_details
    -- ========================================================
    BEGIN
        v_start_time := NOW();
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details FROM '/path/to/datasets/source_crm/sales_details.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        
        v_end_time := NOW();
        v_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        RAISE NOTICE '>> Load Duration: % seconds', ROUND(v_duration_seconds, 2);
        RAISE NOTICE '>> -------';
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'ERROR loading crm_sales_details: % %', SQLSTATE, SQLERRM;
    END;
    
    -- ========================================================
    -- LOADING ERP TABLES
    -- ========================================================
    RAISE NOTICE '';
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';
    
    -- ========================================================
    -- 4. LOAD bronze.erp_loc_a101
    -- ========================================================
    BEGIN
        v_start_time := NOW();
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101 FROM '/path/to/datasets/source_erp/loc_a101.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        
        v_end_time := NOW();
        v_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        RAISE NOTICE '>> Load Duration: % seconds', ROUND(v_duration_seconds, 2);
        RAISE NOTICE '>> -------';
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'ERROR loading erp_loc_a101: % %', SQLSTATE, SQLERRM;
    END;
    
    -- ========================================================
    -- 5. LOAD bronze.erp_cust_az12
    -- ========================================================
    BEGIN
        v_start_time := NOW();
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12 FROM '/path/to/datasets/source_erp/cust_az12.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        
        v_end_time := NOW();
        v_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        RAISE NOTICE '>> Load Duration: % seconds', ROUND(v_duration_seconds, 2);
        RAISE NOTICE '>> -------';
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'ERROR loading erp_cust_az12: % %', SQLSTATE, SQLERRM;
    END;
    
    -- ========================================================
    -- 6. LOAD bronze.erp_px_cat_g1v2
    -- ========================================================
    BEGIN
        v_start_time := NOW();
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2 FROM '/path/to/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        
        v_end_time := NOW();
        v_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        RAISE NOTICE '>> Load Duration: % seconds', ROUND(v_duration_seconds, 2);
        RAISE NOTICE '>> -------';
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'ERROR loading erp_px_cat_g1v2: % %', SQLSTATE, SQLERRM;
    END;
    
    -- ========================================================
    -- COMPLETION LOG
    -- ========================================================
    v_batch_end_time := NOW();
    v_batch_duration_seconds := EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time));
    
    RAISE NOTICE '';
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', ROUND(v_batch_duration_seconds, 2);
    RAISE NOTICE '==========================================';
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Code: %', SQLSTATE;
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE '==========================================';
    RAISE EXCEPTION 'Bronze Layer Load Failed: %', SQLERRM;
END;
$$;

-- =====================================================
-- PROCEDURE EXECUTION
-- =====================================================
-- To execute the stored procedure, use:
-- CALL bronze.load_bronze();

-- =====================================================
-- IMPORTANT NOTES FOR POSTGRESQL
-- =====================================================
/*
1. FILE PATH CONFIGURATION:
   - Update the file paths in the COPY statements to point to your actual CSV files
   - PostgreSQL server must have read permissions on these files
   - Examples:
     Linux/Unix:   '/var/data/csv/cust_info.csv'
     Windows:      'C:/data/csv/cust_info.csv' (use forward slashes or escaped backslashes)
   
2. SUPERUSER REQUIREMENT:
   - COPY FROM with local file paths requires superuser or COPY privilege
   - Alternatively, use COPY FROM STDIN with a client-side loader like psql with \copy command

3. ALTERNATIVE: Using \copy (client-side):
   - In psql client: \copy bronze.crm_cust_info FROM '/path/to/cust_info.csv' WITH (FORMAT csv, HEADER true);
   - This works without superuser privileges

4. COLUMN MAPPING:
   - Ensure CSV column order matches table column definition
   - Or explicitly list columns: COPY table(col1, col2, col3) FROM '...';

5. NULL VALUES:
   - By default, empty strings are converted to NULL
   - Adjust the NULL '' clause if your CSV uses different null markers

6. ERROR HANDLING:
   - Each table load is wrapped in BEGIN...EXCEPTION block
   - Errors are logged but don't stop the entire procedure
   - Check RAISE NOTICE output for individual table error details
*/
