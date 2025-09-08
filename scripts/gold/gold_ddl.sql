/*
=============================================================================
DDL Script: Create Views in Gold Schema
=============================================================================
Script Purpose:
            This script creates views for the Gold layer in the data warehouse.
            The Gold layer represents the final fact and dimension tables (Star Schema)
Each view performs transformations and combines data from the Silver layer to produce a clean,
enriched, and business-ready dataset.
Usage:
- These views can be queried directly for analytics and reporting.
*/
-- ============================================================================
-- Create Dimension: gold.dim_customers
-- ============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW 'gold.dim_customers';
GO
CREATE VIEW gold.dim_customers AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_key) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	lo.cntry AS country,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
	ELSE COALESCE(ce.gen,'N/A')
	END AS gender, -- Data integration for gender column 
	ci.cst_marital_status AS marital_status,
	ce.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ce
ON ci.cst_key = ce.cid
LEFT JOIN silver.erp_loc_a101 lo
ON ci.cst_key = lo.cid;

-- ============================================================================
-- Create Dimension: gold.dim_products
-- ============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW 'gold.dim_products';
GO
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pd.prd_start_dt,pd.prd_key) AS product_key,
	pd.prd_id AS product_id,
	pd.prd_key AS product_number,
	pd.prd_name AS product_name,
	pd.cat_id AS category_id,
	ct.cat AS category,
	ct.subcat AS subcategory,
	ct.maintenance,
	pd.prd_cost AS cost,
	pd.prd_line AS product_line,
	pd.prd_start_dt AS start_date
FROM silver.crm_prd_info pd
LEFT JOIN silver.erp_px_cat_g1v2 ct
ON pd.cat_id = ct.id
WHERE pd.prd_end_dt IS NULL; -- Filter out historical product data

-- ============================================================================
-- Create Fact Table: gold.facrs_sales
-- ============================================================================
IF OBJECT_ID('gold.facts_sales', 'V') IS NOT NULL
    DROP VIEW 'gold.facts_sales';
GO
CREATE VIEW gold.facts_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price 
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


	
