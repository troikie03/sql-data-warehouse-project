/*========================================================================================

						C R E A T I N G   S T O R E D   P R O C E D U R E

========================================================================================*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
/*========================================================================================

							T R U N C A T I N G   T A B L E S 

========================================================================================*/

------------------------------------------------------------------------------------------
									-- S O U R C E   C R M --

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
	SET @batch_start_time = GETDATE();
	
	PRINT '==================================================';
	PRINT 'Loading Silver Layer';
	PRINT '==================================================';

	--SOURCE CRM: cust_info, prd_info, sales_details
	PRINT '--------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '--------------------------------------------------';
	
	--Truncate Table: silver.crm_cust_info
	SET @start_time = GETDATE();
	
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';

	INSERT INTO silver.crm_cust_info 
	(
		cst_id
		,cst_key
		,cst_firstname
		,cst_lastname
		,cst_marital_status
		,cst_gndr
		,cst_create_date
	)

	SELECT
	cst_id
	,cst_key
	,TRIM(cst_firstname) AS cst_firstname
	,TRIM(cst_lastname) AS cst_lastname
	,CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
		END AS cst_marital_status
	,CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
		END AS cst_gndr
	,cst_create_date

	FROM
	(
	SELECT *

		FROM
		(
		SELECT
		*
		,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last

		FROM bronze.crm_cust_info
		
		WHERE cst_id IS NOT NULL
		) a

		WHERE flag_last = 1
	) a;
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '**********************';

	--Truncate Table: silver.crm_prd_info
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info
	(
		prd_id
		,cat_id
		,prd_key
		,prd_nm
		,prd_cost
		,prd_line
		,prd_start_dt
		,prd_end_dt
	)

	SELECT
	prd_id
	,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
	,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
	,prd_nm
	,COALESCE(prd_cost, 0) prd_cost
	,CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'T' THEN 'Touring'
		WHEN 'S' THEN 'Other Sales'
		ELSE 'n/a'
	END AS prd_line
	,CAST(prd_start_dt AS DATE) prd_start_dt
	,CAST(LEAD(prd_start_dt, 1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt

	FROM bronze.crm_prd_info;
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '**********************';

	--Truncate Table: silver.crm_sales_details
	SET @start_time = GETDATE();
	
	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details
	(
		sls_ord_num
		,sls_prd_key
		,sls_cust_id
		,sls_order_dt
		,sls_ship_dt
		,sls_due_dt
		,sls_sales
		,sls_quantity
		,sls_price
	)

	SELECT
	sls_ord_num
	,sls_prd_key
	,sls_cust_id
	,CASE
		WHEN sls_order_dt < 0 OR LEN(sls_order_dt) <> 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	 END sls_order_dt
	,CASE
		WHEN sls_ship_dt < 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	 END sls_ship_dt
	,CASE
		WHEN sls_due_dt < 0 OR LEN(sls_due_dt) <> 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	 END sls_due_dt
	,CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	 END AS sls_sales
	,sls_quantity
	,CASE
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	 END AS sls_price

	FROM bronze.crm_sales_details;
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '**********************';

										-- S O U R C E   C R M --
	------------------------------------------------------------------------------------------

	------------------------------------------------------------------------------------------
										-- S O U R C E   E R P --
										
	----SOURCE ERP: cust_az12, loc_a101, px_cat_g1v2
	PRINT '--------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '--------------------------------------------------';

	--Truncate Table: silver.erp_cust_az12
	SET @start_time = GETDATE();
	
	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12 (CID, BDATE, GEN)

	SELECT
	CASE
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
		ELSE CID
	 END AS CID 
	,CASE
		WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE
	 END AS BEDATE 
	,CASE
		WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
		ELSE 'n/a'
	 END AS GEN

	FROM bronze.erp_cust_az12;
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '**********************';

	--Truncate Table: silver.erp_loc_a101
	SET @start_time = GETDATE();
	
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101 (CID,CNTRY)

	SELECT
	REPLACE(CID, '-', '') CID
	,CASE
		WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
		WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
		WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = ' ' THEN 'n/a'
		ELSE TRIM(CNTRY)
	END CNTRY

	FROM bronze.erp_loc_a101;
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '**********************';

	--Truncate Table: silver.erp_px_cat_g1v2
	SET @start_time = GETDATE();
	
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)

	SELECT * FROM bronze.erp_px_cat_g1v2;
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '**********************';
	
	SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '========================================';
	END TRY
	BEGIN CATCH
		PRINT '==================================================================='
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE()
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR)
		PRINT '==================================================================='
	END CATCH

									-- S O U R C E   E R P --
------------------------------------------------------------------------------------------
END
