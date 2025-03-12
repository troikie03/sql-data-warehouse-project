/*
=================================================================================================
                            	 *** S C R I P T    S U M M A R Y ***
                        	DDL Script: Create Bronze Layer Tables
			Stored Procedure: Load Bronze Layer (Source -> Bronze)
=================================================================================================
DDL Script Purpose: This script creates tables in the 'bronze' schema, dropping existing tables
if they already exist. The stored procedure for the 'bronze' schema is also placed in this script

Run this script to redefine the DDL structure of the 'bronze' tables

Stored Procedure: This stored procedure loads data into the 'bronze schema' from external
CSV files. It performs the following actions
	- Truncates the bronze tables before loading the data
	- Uses the BULK INSERT command to load data from csv files to bronze tables.

USAGE: EXEC bronze.load_bronze
=================================================================================================

*/

EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '==================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '==================================================';

		--SOURCE CRM: cust_info, prd_info, sales_details
			PRINT '--------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '--------------------------------------------------';
	
		--cust_info
		IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
			DROP TABLE bronze.crm_cust_info;
		CREATE TABLE bronze.crm_cust_info
		(
			cst_id 				INT
			,cst_key 			NVARCHAR(50)
			,cst_firstname 		NVARCHAR(50)
			,cst_lastname		NVARCHAR(50)
			,cst_marital_status NVARCHAR(50)
			,cst_gndr 			NVARCHAR(50)
			,cst_create_date 	DATE
		);
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\LENOVO\Downloads\SQL - Data Analytics Project Main\sql-data-warehouse-project-2025\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW			= 2 -- Insert 2 if there is a column header on the table of the source file
			,FIELDTERMINATOR	= ',' -- Used delimiter of the source file (e.g. comma, semicolon, bar, ...)
			,TABLOCK --OPTIONAL
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '**********************';

		--prd_info
		IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
			DROP TABLE bronze.crm_prd_info;
		CREATE TABLE bronze.crm_prd_info
		(
			prd_id 			INT
			,prd_key 		NVARCHAR(50)
			,prd_nm 		NVARCHAR(50)
			,prd_cost 		INT
			,prd_line 		NVARCHAR(50)
			,prd_start_dt 	DATETIME
			,prd_end_dt 	DATETIME
		);

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\LENOVO\Downloads\SQL - Data Analytics Project Main\sql-data-warehouse-project-2025\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW			= 2
			,FIELDTERMINATOR	= ','
			,TABLOCK --OPTIONAL
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '**********************';

		--Sales Details
		IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
			DROP TABLE bronze.crm_sales_details;
		CREATE TABLE bronze.crm_sales_details
		(
			sls_ord_num 	NVARCHAR(50)
			,sls_prd_key 	NVARCHAR(50)
			,sls_cust_id 	INT
			,sls_order_dt 	INT
			,sls_ship_dt 	INT
			,sls_due_dt 	INT
			,sls_sales 		INT
			,sls_quantity 	INT
			,sls_price 		INT
		);
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\LENOVO\Downloads\SQL - Data Analytics Project Main\sql-data-warehouse-project-2025\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW			= 2
			,FIELDTERMINATOR	= ','
			,TABLOCK --OPTIONAL
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '**********************';

		----SOURCE ERP: cust_az12, loc_a101, px_cat_g1v2
			PRINT '--------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '--------------------------------------------------';
	
		--cust_az12
		IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
			DROP TABLE bronze.erp_cust_az12;
		CREATE TABLE bronze.erp_cust_az12
		(
			CID 	NVARCHAR(50)
			,BDATE 	DATE
			,GEN 	NVARCHAR(50)
		);
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\LENOVO\Downloads\SQL - Data Analytics Project Main\sql-data-warehouse-project-2025\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW			= 2
			,FIELDTERMINATOR	= ','
			,TABLOCK --OPTIONAL
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '**********************';

		--loc_a101
		IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
			DROP TABLE bronze.erp_loc_a101;
		CREATE TABLE bronze.erp_loc_a101
		(
			CID 	NVARCHAR(50)
			,CNTRY 	NVARCHAR(50)
		);

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\LENOVO\Downloads\SQL - Data Analytics Project Main\sql-data-warehouse-project-2025\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW			= 2
			,FIELDTERMINATOR	= ','
			,TABLOCK --OPTIONAL
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '**********************';

		--px_cat_g1v2
		IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
			DROP TABLE bronze.erp_px_cat_g1v2;
		CREATE TABLE bronze.erp_px_cat_g1v2
		(
			ID 				NVARCHAR(50)
			,CAT 			NVARCHAR(50)
			,SUBCAT 		NVARCHAR(50)
			,MAINTENANCE 	NVARCHAR(50)
		);
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\LENOVO\Downloads\SQL - Data Analytics Project Main\sql-data-warehouse-project-2025\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW			= 2
			,FIELDTERMINATOR	= ','
			,TABLOCK --OPTIONAL
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '**********************';

		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '========================================';
	END TRY
	BEGIN CATCH
		PRINT '==================================================================='
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE()
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR)
		PRINT '==================================================================='
	END CATCH
END

--Add this script to edit the table for data types and column names then reconstruct the table script from scratch
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
	
--Add this script to avoid value duplication when loading values from a source file
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
