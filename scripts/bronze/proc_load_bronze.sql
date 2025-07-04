/*
==================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
==================================================
*/



Create or Alter Procedure bronze.Load_bronze  as 
BEGIN

	DECLARE @start_time DATETIME , @end_time  DATETIME, @batch_start_time DATETIME , @batch_end_time  DATETIME;
	BEGIN TRY 
		
		SET @batch_start_time = GETDATE();
		PRINT'==============================================================================================================';
		PRINT'                                       Loading Bronce Layer                                                   ';
		PRINT'==============================================================================================================';


		PRINT'-----------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'-----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncting Table :Bronze.crm_cust_info '
		TRUNCATE TABLE  Bronze.crm_cust_info

		PRINT'>> Inserting Data into:Bronze.crm_cust_info '
		BULK INSERT Bronze.crm_cust_info
		From 'C:\Users\dibgh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			Firstrow =2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST ( DATEDIFF(second ,@start_time,@end_time) AS NVARCHAR)  + 'seconds';
		PRINT'>>---------------------------------'

		--SELECT* FROM  Bronze.crm_cust_info
		--SELECT COUNT(*) FROM Bronze.crm_cust_info

	    -------------------------------------------
	    SET @start_time = GETDATE();
		PRINT'>> Truncting Table :Bronze.crm_prd_info '
		TRUNCATE TABLE  Bronze.crm_prd_info

		PRINT'>> Inserting Data into:Bronze.crm_prd_info'
		BULK INSERT Bronze.crm_prd_info
		From 'C:\Users\dibgh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			Firstrow =2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST ( DATEDIFF(second ,@start_time,@end_time) AS NVARCHAR)  + 'seconds';
		PRINT'>>---------------------------------'

		--SELECT* FROM  Bronze.crm_prd_info
		--SELECT COUNT(*) FROM Bronze.crm_prd_info

		-------------------------------------------
		SET @start_time = GETDATE();
		PRINT'>> Truncting Table : Bronze.crm_sales_details'
		TRUNCATE TABLE  Bronze.crm_sales_details
	
		PRINT'>> Inserting Data into:Bronze.crm_sales_details'
		BULK INSERT  Bronze.crm_sales_details
		From 'C:\Users\dibgh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			Firstrow =2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST ( DATEDIFF(second ,@start_time,@end_time) AS NVARCHAR)  + 'seconds';
		PRINT'>>---------------------------------'



		--SELECT* FROM  Bronze.crm_sales_details
		--SELECT COUNT(*) FROM  Bronze.crm_sales_details

		-------------------------------------------------

	
		PRINT'----------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncting Table :Bronze.erp_cust_az12'
		TRUNCATE TABLE Bronze.erp_cust_az12

		PRINT'>> Inserting Data into:Bronze.erp_cust_az12'
		BULK INSERT  Bronze.erp_cust_az12
		From 'C:\Users\dibgh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\Cust_Az12.csv'

		with(
			Firstrow =2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST ( DATEDIFF(second ,@start_time,@end_time) AS NVARCHAR)  + 'seconds';
		PRINT'>>---------------------------------'


		--SELECT* FROM  Bronze.erp_cust_az12
		--SELECT COUNT(*) FROM Bronze.erp_cust_az12

		---------------------------------------------
	
		SET @start_time = GETDATE();
		PRINT'>> Truncting Table :Bronze.erp_loc_a101'
		TRUNCATE TABLE Bronze.erp_loc_a101

	
		PRINT'>> Inserting Data into:Bronze.erp_loc_a101'
		BULK INSERT   Bronze.erp_loc_a101
		From 'C:\Users\dibgh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'

		with(
			Firstrow =2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST ( DATEDIFF(second ,@start_time,@end_time) AS NVARCHAR)  + 'seconds';
		PRINT'>>---------------------------------'

		--SELECT* FROM   Bronze.erp_loc_a101
		--SELECT COUNT(*) FROM  Bronze.erp_loc_a101

		---------------------------------------------
		SET @start_time = GETDATE();
		PRINT'>> Truncting Table :Bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2
	
		PRINT'>> Inserting Data into:Bronze.erp_px_cat_g1v2'
		BULK INSERT  Bronze.erp_px_cat_g1v2
		From 'C:\Users\dibgh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'

		with(
			Firstrow =2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST ( DATEDIFF(second ,@start_time,@end_time) AS NVARCHAR)  + 'seconds';
		PRINT'>>---------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '==========================================='
		PRINT 'Loading  BRONZE LAYER is Completed'
	    PRINT'>> - Total Load Duration: ' + CAST ( DATEDIFF(second ,@batch_start_time, @batch_end_time) AS NVARCHAR)  + 'seconds';
		PRINT '==========================================='



	END TRY
	BEGIN CATCH
		PRINT '==========================================='
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================='


	END CATCH


END

	--SELECT* FROM  Bronze.erp_px_cat_g1v2
	--SELECT COUNT(*) FROM   Bronze.erp_px_cat_g1v2
	------------------------------------------------
