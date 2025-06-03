/*
=================================================================================
Quality Checks
=================================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the ‘silver’ schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
=================================================================================
*/

--=======================================
-- silver.crm_cust_info Checks
--=======================================

-- NULLs or Duplicates in Primary Key
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Unwanted Spaces
SELECT cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname != TRIM(cst_lastname);

-- Data Standardization
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

-- Bronze: Show NULL/Duplicate rows
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last != 1;

-- Bronze: Keep only latest valid entry
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;

--=======================================
-- silver.crm_prd_info Checks
--=======================================

-- NULLs or Duplicates in Primary Key
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Unwanted Spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- NULL or Negative Values in Cost
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Invalid Date Ranges
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Data Standardization
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

--=======================================
-- silver.crm_sales_details Checks
--=======================================

-- Full Data Check
SELECT * FROM silver.crm_sales_details;

-- Invalid Date Orders
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Invalid Dates from Bronze
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
   OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

-- Bronze: Cross Date Consistency
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Sales, Quantity, Price Consistency
SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;

--=======================================
-- silver.erp_cust_az12 Checks
--=======================================

-- Out-of-range Dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Standardization
SELECT DISTINCT gen FROM silver.erp_cust_az12;

-- Full Table
SELECT * FROM silver.erp_cust_az12;

--=======================================
-- silver.erp_loc_a101 Checks
--=======================================

-- Country Standardization
SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry;

-- Full Table
SELECT * FROM silver.erp_loc_a101;

--=======================================
-- silver.erp_px_cat_g1v2 Checks
--=======================================

-- Unwanted Spaces from Bronze
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Standardization
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;
