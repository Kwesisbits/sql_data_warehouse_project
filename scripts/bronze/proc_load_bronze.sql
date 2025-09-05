/*
==================================================================================
Stored Procedure: Load Bronze Layer (Source-> Bronze Shema)
==================================================================================
Script Purpose:
    This stored procedure loads data into the bronze schema from external CSV files.
    - It truncates the table before loading data to avoid duplicated data from running more than once
    - It uses 'BULK INSERT' command data to load data from CSV files into bronze shema tables 

Parameters: None.
This stored procedure does not accept any parameters or return any values 

Usage Example:
  EXEC bronze.load_bronze 
  --- implememted after running stored procedure script to load the data;
==================================================================================
*/

-- Creates stored procedure to load tables with data from source

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @t_start DATETIME, @t_end DATETIME
	BEGIN TRY 
		SET @t_start =GETDATE()
		PRINT '=======================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=======================================';
		PRINT '****************************************';
		PRINT 'Loading CRM Tables';
		PRINT '****************************************';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		); 
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		); 
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------'

		PRINT '****************************************'
		PRINT 'Loading ERP Tables'
		PRINT '****************************************'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		); 
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------'

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		); 
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------'

		SET @t_end = GETDATE()
		PRINT '=================================='
		PRINT '>> LOADING DATA IS COMPLETED'
		PRINT '>> Full Batch Load Duration: ' + CAST(DATEDIFF(second, @t_start,@t_end) AS NVARCHAR) + 'seconds';
		PRINT '=================================='

	END TRY 
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING OF BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
