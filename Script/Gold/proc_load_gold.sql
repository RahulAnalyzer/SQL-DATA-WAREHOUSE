
-- =====================================================
-- GOLD LAYER VIEWS - DATA WAREHOUSE
-- Purpose: Create dimensional and fact views for analytics
-- PostgreSQL Compatible
-- =====================================================

-- =====================================================
-- DROP EXISTING VIEWS (if needed)
-- =====================================================
-- Uncomment to drop and recreate views
-- DROP VIEW IF EXISTS gold.fact_sales CASCADE;
-- DROP VIEW IF EXISTS gold.dim_products CASCADE;
-- DROP VIEW IF EXISTS gold.dim_customer CASCADE;

-- =====================================================
-- VIEW 1: DIMENSION - CUSTOMER
-- =====================================================
-- Description: Master customer dimension combining CRM and ERP data
-- Contains: Customer identifiers, demographics, contact info, location
-- Joins: 
--   - silver.crm_cust_info (primary source)
--   - silver.erp_cust_az12 (birthday, additional gender info)
--   - silver.erp_loc_a101 (country/location info)
-- =====================================================

CREATE OR REPLACE VIEW gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthday,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

-- =====================================================
-- VIEW 2: DIMENSION - PRODUCTS
-- =====================================================
-- Description: Master product dimension with category and cost info
-- Contains: Product identifiers, descriptions, categories, pricing, validity dates
-- Joins:
--   - silver.crm_prd_info (primary source)
--   - silver.erp_px_cat_g1v2 (category, subcategory, maintenance info)
-- Filters: Only active products (prd_end_dt IS NULL)
-- =====================================================

CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS product_cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

-- =====================================================
-- VIEW 3: FACT TABLE - SALES
-- =====================================================
-- Description: Fact table containing all sales transactions
-- Contains: Order details, product references, customer references, amounts
-- Joins:
--   - silver.crm_sales_details (primary source - fact data)
--   - gold.dim_products (product dimension)
--   - gold.dim_customer (customer dimension)
-- Grain: One row per order line item
-- =====================================================

CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
    ON sd.sls_cust_id = cu.customer_id;

-- =====================================================
-- VERIFICATION QUERIES (Optional - for testing)
-- =====================================================
/*
-- View Customer Dimension (sample)
SELECT * FROM gold.dim_customer LIMIT 10;

-- View Products Dimension (sample)
SELECT * FROM gold.dim_products LIMIT 10;

-- View Sales Fact Table (sample)
SELECT * FROM gold.fact_sales LIMIT 10;

-- Verify row counts
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL
SELECT 'dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM gold.fact_sales;

-- Sample analytics query - Sales by Country
SELECT 
    c.country,
    COUNT(DISTINCT s.order_number) AS total_orders,
    COUNT(s.order_number) AS total_line_items,
    ROUND(SUM(s.sales_amount)::NUMERIC, 2) AS total_sales,
    ROUND(AVG(s.price)::NUMERIC, 2) AS avg_price
FROM gold.fact_sales s
JOIN gold.dim_customer c ON s.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;

-- Sample analytics query - Sales by Product Line
SELECT 
    p.product_line,
    p.category,
    COUNT(DISTINCT s.order_number) AS total_orders,
    SUM(s.quantity) AS total_quantity_sold,
    ROUND(SUM(s.sales_amount)::NUMERIC, 2) AS total_sales
FROM gold.fact_sales s
JOIN gold.dim_products p ON s.product_key = p.product_key
GROUP BY p.product_line, p.category
ORDER BY total_sales DESC;
*/

-- =====================================================
-- VIEW METADATA & INFORMATION
-- =====================================================
/*
PostgreSQL Version Requirements:
  - Supports VIEW creation: All versions
  - Supports ROW_NUMBER() window function: PostgreSQL 8.4+
  - Supports LEFT JOIN: All versions
  - Supports CASE WHEN: All versions

View Dependencies:
  1. gold.dim_customer (no dependencies within views)
  2. gold.dim_products (no dependencies within views)
  3. gold.fact_sales (depends on gold.dim_customer and gold.dim_products)

Schema Requirements:
  - Requires: bronze schema, silver schema, gold schema
  - Tables required in silver:
    * crm_cust_info
    * crm_prd_info
    * crm_sales_details
    * erp_cust_az12
    * erp_loc_a101
    * erp_px_cat_g1v2

Data Flow:
  Bronze Layer (raw data) → Silver Layer (cleaned data) → Gold Layer (dimensions & facts)
*/

-- =====================================================
-- GRANTS & PERMISSIONS (Optional)
-- =====================================================
/*
-- Grant SELECT permissions to analyst role
GRANT SELECT ON gold.dim_customer TO analyst_role;
GRANT SELECT ON gold.dim_products TO analyst_role;
GRANT SELECT ON gold.fact_sales TO analyst_role;

-- Grant SELECT on gold schema to business users
GRANT USAGE ON SCHEMA gold TO business_users;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO business_users;
*/
