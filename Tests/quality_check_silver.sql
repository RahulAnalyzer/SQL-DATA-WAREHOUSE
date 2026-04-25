
/*
===============================================================================
Quality Checks - Silver Layer
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

PostgreSQL Version: All versions
Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
    - Each section returns expected results (no rows = good data quality)

===============================================================================
*/

-- ====================================================================
-- CHECKING 'silver.crm_cust_info'
-- ====================================================================

-- 1. Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results (should return empty result set)
RAISE NOTICE '';
RAISE NOTICE '========== Checking silver.crm_cust_info ==========';
RAISE NOTICE 'Check 1: Primary Key Uniqueness (NULLs or Duplicates)';

SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

RAISE NOTICE 'Check 1 Complete: If no rows above, primary key is clean.';

-- 2. Check for Unwanted Spaces in customer key
-- Expectation: No Results (should return empty result set)
RAISE NOTICE 'Check 2: Unwanted Spaces in Customer Key';

SELECT 
    cst_key,
    LENGTH(cst_key) AS key_length
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

RAISE NOTICE 'Check 2 Complete: If no rows above, spaces are trimmed.';

-- 3. Data Standardization & Consistency for marital status
-- Expectation: Should only show: 'Single', 'Married', 'n/a'
RAISE NOTICE 'Check 3: Marital Status Values (Expected: Single, Married, n/a)';

SELECT DISTINCT 
    cst_marital_status,
    COUNT(*) AS value_count
FROM silver.crm_cust_info
GROUP BY cst_marital_status
ORDER BY cst_marital_status;

RAISE NOTICE 'Check 3 Complete: Review distinct values above.';

-- ====================================================================
-- CHECKING 'silver.crm_prd_info'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking silver.crm_prd_info ==========';

-- 1. Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
RAISE NOTICE 'Check 1: Primary Key Uniqueness (NULLs or Duplicates)';

SELECT 
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

RAISE NOTICE 'Check 1 Complete: If no rows above, primary key is clean.';

-- 2. Check for Unwanted Spaces in product name
-- Expectation: No Results
RAISE NOTICE 'Check 2: Unwanted Spaces in Product Name';

SELECT 
    prd_nm,
    LENGTH(prd_nm) AS name_length
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

RAISE NOTICE 'Check 2 Complete: If no rows above, names are trimmed.';

-- 3. Check for NULLs or Negative Values in Cost
-- Expectation: No Results
RAISE NOTICE 'Check 3: NULL or Negative Product Costs';

SELECT 
    prd_id,
    prd_nm,
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

RAISE NOTICE 'Check 3 Complete: If no rows above, costs are valid.';

-- 4. Data Standardization & Consistency for product line
-- Expectation: Should only show: 'Mountain', 'Road', 'Other Sales', 'Touring', 'n/a'
RAISE NOTICE 'Check 4: Product Line Values (Expected: Mountain, Road, Other Sales, Touring, n/a)';

SELECT DISTINCT 
    prd_line,
    COUNT(*) AS value_count
FROM silver.crm_prd_info
GROUP BY prd_line
ORDER BY prd_line;

RAISE NOTICE 'Check 4 Complete: Review distinct values above.';

-- 5. Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
RAISE NOTICE 'Check 5: Invalid Date Orders (Start Date > End Date)';

SELECT 
    prd_id,
    prd_nm,
    prd_start_dt,
    prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

RAISE NOTICE 'Check 5 Complete: If no rows above, date ranges are valid.';

-- ====================================================================
-- CHECKING 'silver.crm_sales_details'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking silver.crm_sales_details ==========';

-- 1. Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
RAISE NOTICE 'Check 1: Invalid Date Orders (Order > Ship or Due)';

SELECT 
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt
LIMIT 20;

RAISE NOTICE 'Check 1 Complete: If no rows above, date sequence is valid.';

-- 2. Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results (all calculations should match)
RAISE NOTICE 'Check 2: Sales Amount = Quantity × Price Consistency';

SELECT 
    sls_ord_num,
    sls_quantity,
    sls_price,
    (sls_quantity * sls_price)::NUMERIC(15, 2) AS calculated_sales,
    sls_sales,
    ROUND((sls_sales - (sls_quantity * sls_price))::NUMERIC, 2) AS difference
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_ord_num
LIMIT 20;

RAISE NOTICE 'Check 2 Complete: If no rows above, sales calculations are consistent.';

-- 3. Summary of sales data quality
-- Expected: All values > 0, no NULLs
RAISE NOTICE 'Check 3: Sales Data Summary Statistics';

SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN sls_sales IS NULL THEN 1 END) AS null_sales,
    COUNT(CASE WHEN sls_quantity IS NULL THEN 1 END) AS null_quantity,
    COUNT(CASE WHEN sls_price IS NULL THEN 1 END) AS null_price,
    COUNT(CASE WHEN sls_sales <= 0 THEN 1 END) AS zero_or_negative_sales,
    MIN(sls_sales) AS min_sales,
    MAX(sls_sales) AS max_sales,
    ROUND(AVG(sls_sales)::NUMERIC, 2) AS avg_sales
FROM silver.crm_sales_details;

RAISE NOTICE 'Check 3 Complete: Review statistics above.';

-- ====================================================================
-- CHECKING 'silver.erp_cust_az12'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking silver.erp_cust_az12 ==========';

-- 1. Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
RAISE NOTICE 'Check 1: Out-of-Range Birthdates (Expected: 1924-01-01 to Today)';

SELECT 
    cid,
    bdate,
    EXTRACT(YEAR FROM bdate) AS birth_year,
    EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM bdate) AS approximate_age
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > NOW()::DATE
LIMIT 20;

RAISE NOTICE 'Check 1 Complete: If no rows above, birthdates are valid.';

-- 2. Data Standardization & Consistency for gender
-- Expected: 'Male', 'Female', 'n/a'
RAISE NOTICE 'Check 2: Gender Values (Expected: Male, Female, n/a)';

SELECT DISTINCT 
    gen,
    COUNT(*) AS value_count
FROM silver.erp_cust_az12
GROUP BY gen
ORDER BY gen;

RAISE NOTICE 'Check 2 Complete: Review distinct values above.';

-- 3. Summary statistics for birthdates
RAISE NOTICE 'Check 3: Birthdate Summary Statistics';

SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN bdate IS NULL THEN 1 END) AS null_birthdates,
    MIN(bdate) AS oldest_birthdate,
    MAX(bdate) AS most_recent_birthdate,
    ROUND(AVG(EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM bdate))::NUMERIC, 1) AS avg_age
FROM silver.erp_cust_az12
WHERE bdate IS NOT NULL;

RAISE NOTICE 'Check 3 Complete: Review statistics above.';

-- ====================================================================
-- CHECKING 'silver.erp_loc_a101'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking silver.erp_loc_a101 ==========';

-- Data Standardization & Consistency for countries
RAISE NOTICE 'Check 1: Country Values and Distribution';

SELECT 
    cntry,
    COUNT(*) AS location_count
FROM silver.erp_loc_a101
GROUP BY cntry
ORDER BY cntry;

RAISE NOTICE 'Check 1 Complete: Review distinct countries above.';

-- Check for n/a or missing values
RAISE NOTICE 'Check 2: Missing or N/A Country Values';

SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN cntry = 'n/a' THEN 1 END) AS n_a_count,
    COUNT(CASE WHEN cntry IS NULL THEN 1 END) AS null_count
FROM silver.erp_loc_a101;

RAISE NOTICE 'Check 2 Complete: Review missing values above.';

-- ====================================================================
-- CHECKING 'silver.erp_px_cat_g1v2'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking silver.erp_px_cat_g1v2 ==========';

-- 1. Check for Unwanted Spaces
-- Expectation: No Results
RAISE NOTICE 'Check 1: Unwanted Spaces in Category Fields';

SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

RAISE NOTICE 'Check 1 Complete: If no rows above, trimming is complete.';

-- 2. Data Standardization & Consistency for maintenance
-- Expected: Various maintenance codes/values
RAISE NOTICE 'Check 2: Maintenance Values and Distribution';

SELECT DISTINCT 
    maintenance,
    COUNT(*) AS category_count
FROM silver.erp_px_cat_g1v2
GROUP BY maintenance
ORDER BY maintenance;

RAISE NOTICE 'Check 2 Complete: Review distinct maintenance values above.';

-- 3. Category and Subcategory Summary
RAISE NOTICE 'Check 3: Category and Subcategory Summary';

SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT cat) AS unique_categories,
    COUNT(DISTINCT subcat) AS unique_subcategories,
    COUNT(DISTINCT maintenance) AS unique_maintenance_values
FROM silver.erp_px_cat_g1v2;

RAISE NOTICE 'Check 3 Complete: Review summary statistics above.';

-- ====================================================================
-- OVERALL SILVER LAYER SUMMARY
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== SILVER LAYER OVERALL SUMMARY ==========';

SELECT 
    'crm_cust_info' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT cst_id) AS unique_primary_keys
FROM silver.crm_cust_info

UNION ALL

SELECT 
    'crm_prd_info',
    COUNT(*),
    COUNT(DISTINCT prd_id)
FROM silver.crm_prd_info

UNION ALL

SELECT 
    'crm_sales_details',
    COUNT(*),
    COUNT(DISTINCT sls_ord_num)
FROM silver.crm_sales_details

UNION ALL

SELECT 
    'erp_cust_az12',
    COUNT(*),
    COUNT(DISTINCT cid)
FROM silver.erp_cust_az12

UNION ALL

SELECT 
    'erp_loc_a101',
    COUNT(*),
    COUNT(DISTINCT cid)
FROM silver.erp_loc_a101

UNION ALL

SELECT 
    'erp_px_cat_g1v2',
    COUNT(*),
    COUNT(DISTINCT id)
FROM silver.erp_px_cat_g1v2;

RAISE NOTICE '========== QUALITY CHECKS COMPLETE ==========';
RAISE NOTICE 'All checks above completed. Review any unexpected results.';