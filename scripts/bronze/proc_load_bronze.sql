/*
============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================
Script Purpose:
     This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performes the following actions:
  -Truncate the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.
Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
=============================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================================';
		print 'Loading Bronze Layer';
		print '=====================================================';

		print '-----------------------------------------------------';
		print 'Lading CRM Tables';
		print '-----------------------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE Table bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\srivalli\OneDrive\Documents\dev\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE Table bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\srivalli\OneDrive\Documents\dev\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE Table bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\srivalli\OneDrive\Documents\dev\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-------------------------------------';


		print '-----------------------------------------------------';
		print 'Lading ERP Tables';
		print '-----------------------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE Table bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\srivalli\OneDrive\Documents\dev\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-------------------------------------';


		SET @start_time = GETDATE();
		TRUNCATE Table bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\srivalli\OneDrive\Documents\dev\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE Table bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\srivalli\OneDrive\Documents\dev\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '-------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '======================================================'
		PRINT '>> Loading Bronze Layer Completed';
		PRINT 'Total load Duration '+CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '======================================================'
	END TRY
	BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR Message' + ERROR_MESSAGE();
		PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR Message' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
