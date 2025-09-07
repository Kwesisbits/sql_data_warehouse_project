/*

*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @t_start DATETIME, @t_end DATETIME
	BEGIN TRY
		SET @t_start = GETDATE()
		PRINT '=======================================';
		PRINT 'Loading Silver Layer';
		PRINT '=======================================';
		PRINT '****************************************';
		PRINT 'Loading CRM Tables';
		PRINT '****************************************';

		PRINT '>> Truncating Table: silver.crm_cust_info'
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.crm_cust_info 
		PRINT '>> Insert Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
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
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 ELSE 'N/A' -- data standardization: map marital status code with descriptive values 
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'N/A' -- data standardization: map gender code with descriptive values 
		END cst_gndr,
		cst_create_date
		FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as last_flag
		FROM bronze.crm_cust_info  
		)t 
		WHERE last_flag = 1 AND cst_id IS NOT NULL;-- removes duplicates in data by selecting latest data for duplicates
		SET @end_time = GETDATE()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> --------------------------------'

		PRINT '>> Truncating Table: silver.crm_prd_info'
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.crm_prd_info 
		PRINT '>> Insert Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- key for join with erp category data
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- key for join with crm sales details
			prd_name,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line)) 
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'R' THEN 'Road'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'Uknown' -- Map product line codes to descriptive values
			END prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
			) AS prd_end_dt --calculate the end date as a one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> --------------------------------'

		PRINT '>> Truncating Table: silver.crm_sales_details'
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.crm_sales_details 
		PRINT '>> Insert Data Into: silver.crm_sales_details'
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
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt, -- Corrects date format and removes incorrect date data
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price 
				THEN sls_quantity * ABS(sls_price) -- Removes and corrects incorrect sales data
			ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 
				THEN ABS(sls_sales)/NULLIF(sls_quantity,0) -- Removes and corrects incorrect price data
			ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> --------------------------------'

		PRINT '****************************************'
		PRINT 'Loading ERP Tables'
		PRINT '****************************************'

		PRINT '>> Truncating Table: silver.erp_cust_az12'
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.erp_cust_az12 
		PRINT '>> Insert Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
			ELSE cid
			END AS cid, -- Data improvement for synchrony with parallel key for planned join
			CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
			END AS bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				 ELSE 'N/A' -- Normalize gender data 
			END AS gen
		FROM bronze.erp_cust_az12


		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Insert Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (cid,cntry)
		SELECT
		REPLACE(cid,'-','') cid, -- Data improvement for synchrony with parallel key for planned join
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'United States'
			 WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
			 ELSE TRIM(cntry) -- Normalize country data
		END cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> --------------------------------';

		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		SET @start_time = GETDATE()
		TRUNCATE TABLE silver.erp_px_cat_g1v2 
		PRINT '>> Insert Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenenace
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> --------------------------------'

		SET @t_end = GETDATE()
		PRINT '=================================='
		PRINT '>> TRANSFORMING & LOADING DATA IS COMPLETED'
		PRINT '>> Full Batch Load Duration: ' + CAST(DATEDIFF(second, @t_start,@t_end) AS NVARCHAR) + 'seconds';
		PRINT '=================================='
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING OF SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END

