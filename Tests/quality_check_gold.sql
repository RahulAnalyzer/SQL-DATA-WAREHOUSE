/*
===============================================================================
Quality Checks - Gold Layer
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.
    - Data completeness and validity.

PostgreSQL Version: All versions
Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
    - Each section returns expected results (no rows = good data quality)
    - Dimension tables should have unique keys
    - Fact table should reference valid dimension keys

===============================================================================
*/

-- ====================================================================
-- CHECKING 'gold.dim_customer'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking gold.dim_customer ==========';

-- 1. Check for Uniqueness of Customer Key
-- Expectation: No Results (all customer_key values should be unique)
RAISE NOTICE 'Check 1: Customer Key Uniqueness';

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;

RAISE NOTICE 'Check 1 Complete: If no rows above, all customer keys are unique.';

-- 2. Check for Uniqueness of Customer ID (business key)
-- Expectation: No Results or minimal duplicates (if customer appears once per time period)
RAISE NOTICE 'Check 2: Customer ID (Business Key) Distribution';

SELECT 
    customer_id,
    COUNT(*) AS occurrence_count
FROM gold.dim_customer
GROUP BY customer_id
ORDER BY occurrence_count DESC
LIMIT 10;

RAISE NOTICE 'Check 2 Complete: Review customer ID occurrences above.';

-- 3. Check for Missing Required Fields
-- Expectation: No Results
RAISE NOTICE 'Check 3: Missing Required Customer Fields';

SELECT 
    customer_key,
    customer_id,
    CASE 
        WHEN first_name IS NULL THEN 'Missing first_name'
        WHEN last_name IS NULL THEN 'Missing last_name'
        WHEN customer_number IS NULL THEN 'Missing customer_number'
    END AS missing_field
FROM gold.dim_customer
WHERE first_name IS NULL 
   OR last_name IS NULL 
   OR customer_number IS NULL;

RAISE NOTICE 'Check 3 Complete: If no rows above, all required fields populated.';

-- 4. Check for Data Standardization
-- Expected: gender = 'Male', 'Female', or 'n/a'
RAISE NOTICE 'Check 4: Gender Standardization (Expected: Male, Female, n/a)';

SELECT DISTINCT 
    gender,
    COUNT(*) AS customer_count
FROM gold.dim_customer
GROUP BY gender
ORDER BY gender;

RAISE NOTICE 'Check 4 Complete: Review distinct gender values above.';

-- 5. Check for Data Standardization
-- Expected: marital_status = 'Single', 'Married', or 'n/a'
RAISE NOTICE 'Check 5: Marital Status Standardization (Expected: Single, Married, n/a)';

SELECT DISTINCT 
    marital_status,
    COUNT(*) AS customer_count
FROM gold.dim_customer
GROUP BY marital_status
ORDER BY marital_status;

RAISE NOTICE 'Check 5 Complete: Review distinct marital status values above.';

-- 6. Check for Country Distribution
RAISE NOTICE 'Check 6: Country Distribution';

SELECT 
    country,
    COUNT(*) AS customer_count
FROM gold.dim_customer
GROUP BY country
ORDER BY customer_count DESC;

RAISE NOTICE 'Check 6 Complete: Review country distribution above.';

-- 7. Dimension Summary
RAISE NOTICE 'Check 7: Customer Dimension Summary';

SELECT 
    COUNT(*) AS total_customers,
    COUNT(DISTINCT customer_id) AS unique_customer_ids,
    COUNT(DISTINCT customer_key) AS unique_customer_keys,
    COUNT(CASE WHEN birthday IS NULL THEN 1 END) AS null_birthdates,
    COUNT(CASE WHEN country IS NULL OR country = 'n/a' THEN 1 END) AS missing_country
FROM gold.dim_customer;

RAISE NOTICE 'Check 7 Complete: Review customer dimension summary above.';

-- ====================================================================
-- CHECKING 'gold.dim_products'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking gold.dim_products ==========';

-- 1. Check for Uniqueness of Product Key
-- Expectation: No Results (all product_key values should be unique)
RAISE NOTICE 'Check 1: Product Key Uniqueness';

SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

RAISE NOTICE 'Check 1 Complete: If no rows above, all product keys are unique.';

-- 2. Check for Uniqueness of Product ID (business key)
-- Expectation: No Results (each product should appear once, or once per time period)
RAISE NOTICE 'Check 2: Product ID (Business Key) Distribution';

SELECT 
    product_id,
    COUNT(*) AS occurrence_count
FROM gold.dim_products
GROUP BY product_id
ORDER BY occurrence_count DESC
LIMIT 10;

RAISE NOTICE 'Check 2 Complete: Review product ID occurrences above.';

-- 3. Check for Missing Required Fields
-- Expectation: No Results
RAISE NOTICE 'Check 3: Missing Required Product Fields';

SELECT 
    product_key,
    product_id,
    CASE 
        WHEN product_name IS NULL THEN 'Missing product_name'
        WHEN category IS NULL THEN 'Missing category'
        WHEN product_number IS NULL THEN 'Missing product_number'
    END AS missing_field
FROM gold.dim_products
WHERE product_name IS NULL 
   OR category IS NULL 
   OR product_number IS NULL;

RAISE NOTICE 'Check 3 Complete: If no rows above, all required fields populated.';

-- 4. Check for Invalid Cost Values
-- Expected: No negative or null costs
RAISE NOTICE 'Check 4: Invalid Product Costs';

SELECT 
    product_key,
    product_name,
    product_cost
FROM gold.dim_products
WHERE product_cost < 0 OR product_cost IS NULL;

RAISE NOTICE 'Check 4 Complete: If no rows above, all costs are valid.';

-- 5. Check Product Line Standardization
-- Expected: 'Mountain', 'Road', 'Other Sales', 'Touring', or 'n/a'
RAISE NOTICE 'Check 5: Product Line Standardization';

SELECT DISTINCT 
    product_line,
    COUNT(*) AS product_count
FROM gold.dim_products
GROUP BY product_line
ORDER BY product_line;

RAISE NOTICE 'Check 5 Complete: Review product line values above.';

-- 6. Check Category and Subcategory Distribution
RAISE NOTICE 'Check 6: Category and Subcategory Distribution';

SELECT 
    category,
    subcategory,
    COUNT(*) AS product_count
FROM gold.dim_products
GROUP BY category, subcategory
ORDER BY category, subcategory;

RAISE NOTICE 'Check 6 Complete: Review category distribution above.';

-- 7. Dimension Summary
RAISE NOTICE 'Check 7: Product Dimension Summary';

SELECT 
    COUNT(*) AS total_products,
    COUNT(DISTINCT product_id) AS unique_product_ids,
    COUNT(DISTINCT product_key) AS unique_product_keys,
    ROUND(MIN(product_cost)::NUMERIC, 2) AS min_cost,
    ROUND(AVG(product_cost)::NUMERIC, 2) AS avg_cost,
    ROUND(MAX(product_cost)::NUMERIC, 2) AS max_cost,
    COUNT(CASE WHEN category IS NULL OR category = 'n/a' THEN 1 END) AS missing_category
FROM gold.dim_products;

RAISE NOTICE 'Check 7 Complete: Review product dimension summary above.';

-- ====================================================================
-- CHECKING 'gold.fact_sales'
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== Checking gold.fact_sales ==========';

-- 1. Check Referential Integrity - Missing Product Keys
-- Expectation: No Results (all product references should exist in dim_products)
RAISE NOTICE 'Check 1: Referential Integrity - Missing Product References';

SELECT 
    COUNT(*) AS orphaned_product_records
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

RAISE NOTICE 'Check 1 Complete: If count = 0, all product references are valid.';

-- 2. Check Referential Integrity - Missing Customer Keys
-- Expectation: No Results (all customer references should exist in dim_customer)
RAISE NOTICE 'Check 2: Referential Integrity - Missing Customer References';

SELECT 
    COUNT(*) AS orphaned_customer_records
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
    ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

RAISE NOTICE 'Check 2 Complete: If count = 0, all customer references are valid.';

-- 3. Detailed Orphaned Record Check
-- Expectation: No Results
RAISE NOTICE 'Check 3: Detailed Orphaned Records (Missing Dimension References)';

SELECT 
    f.order_number,
    f.product_key,
    f.customer_key,
    CASE WHEN p.product_key IS NULL THEN 'ORPHANED_PRODUCT' END AS product_status,
    CASE WHEN c.customer_key IS NULL THEN 'ORPHANED_CUSTOMER' END AS customer_status
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON f.product_key = p.product_key
LEFT JOIN gold.dim_customer c
    ON f.customer_key = c.customer_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
LIMIT 20;

RAISE NOTICE 'Check 3 Complete: If no rows above, referential integrity is maintained.';

-- 4. Check for Invalid Sales Amounts
-- Expected: All sales > 0
RAISE NOTICE 'Check 4: Invalid Sales Amounts';

SELECT 
    order_number,
    quantity,
    price,
    sales_amount
FROM gold.fact_sales
WHERE sales_amount <= 0 
   OR sales_amount IS NULL
   OR quantity <= 0 
   OR price <= 0
LIMIT 20;

RAISE NOTICE 'Check 4 Complete: If no rows above, all sales amounts are valid.';

-- 5. Check for Invalid Date Sequences
-- Expected: order_date <= shipping_date <= due_date
RAISE NOTICE 'Check 5: Invalid Date Sequences (Order > Ship or Ship > Due)';

SELECT 
    order_number,
    order_date,
    shipping_date,
    due_date
FROM gold.fact_sales
WHERE order_date > shipping_date 
   OR order_date > due_date
   OR (shipping_date > due_date AND due_date IS NOT NULL)
LIMIT 20;

RAISE NOTICE 'Check 5 Complete: If no rows above, date sequences are valid.';

-- 6. Check for Missing Required Fields
-- Expectation: No Results
RAISE NOTICE 'Check 6: Missing Required Sales Fields';

SELECT 
    COUNT(*) AS missing_field_count
FROM gold.fact_sales
WHERE order_number IS NULL 
   OR product_key IS NULL 
   OR customer_key IS NULL 
   OR order_date IS NULL 
   OR sales_amount IS NULL 
   OR quantity IS NULL;

RAISE NOTICE 'Check 6 Complete: If count = 0, all required fields are populated.';

-- 7. Sales Amount Consistency Check
-- Expected: sales_amount ≈ quantity * price
RAISE NOTICE 'Check 7: Sales Amount Calculation Consistency';

SELECT 
    order_number,
    quantity,
    price,
    (quantity * price)::NUMERIC(15, 2) AS calculated_amount,
    sales_amount,
    ROUND((sales_amount - (quantity * price))::NUMERIC, 2) AS variance
FROM gold.fact_sales
WHERE ABS(sales_amount - (quantity * price)) > 0.01
LIMIT 20;

RAISE NOTICE 'Check 7 Complete: If no rows above, calculations are consistent.';

-- 8. Fact Table Summary
RAISE NOTICE 'Check 8: Fact Table Summary';

SELECT 
    COUNT(*) AS total_line_items,
    COUNT(DISTINCT order_number) AS unique_orders,
    COUNT(DISTINCT product_key) AS referenced_products,
    COUNT(DISTINCT customer_key) AS referenced_customers,
    ROUND(MIN(sales_amount)::NUMERIC, 2) AS min_sales,
    ROUND(AVG(sales_amount)::NUMERIC, 2) AS avg_sales,
    ROUND(MAX(sales_amount)::NUMERIC, 2) AS max_sales,
    ROUND(SUM(sales_amount)::NUMERIC, 2) AS total_sales,
    MIN(order_date) AS earliest_order,
    MAX(order_date) AS latest_order
FROM gold.fact_sales;

RAISE NOTICE 'Check 8 Complete: Review fact table summary above.';

-- ====================================================================
-- OVERALL GOLD LAYER SUMMARY
-- ====================================================================

RAISE NOTICE '';
RAISE NOTICE '========== GOLD LAYER OVERALL SUMMARY ==========';

WITH dim_customer_stats AS (
    SELECT 
        'dim_customer' AS object_name,
        'DIMENSION' AS object_type,
        COUNT(*) AS record_count,
        0 AS orphaned_records
    FROM gold.dim_customer
),
dim_products_stats AS (
    SELECT 
        'dim_products',
        'DIMENSION',
        COUNT(*),
        0
    FROM gold.dim_products
),
fact_sales_stats AS (
    SELECT 
        'fact_sales',
        'FACT',
        COUNT(*),
        (SELECT COUNT(*) FROM gold.fact_sales f 
         LEFT JOIN gold.dim_products p ON f.product_key = p.product_key 
         LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key 
         WHERE p.product_key IS NULL OR c.customer_key IS NULL)
    FROM gold.fact_sales
)
SELECT * FROM dim_customer_stats
UNION ALL
SELECT * FROM dim_products_stats
UNION ALL
SELECT * FROM fact_sales_stats;

RAISE NOTICE '========== QUALITY CHECKS COMPLETE ==========';
RAISE NOTICE 'All checks above completed. Review any unexpected results.';
RAISE NOTICE 'No results in checks = Good Data Quality ✓';