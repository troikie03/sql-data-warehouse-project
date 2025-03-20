/*=================================================================================

Q U A L I T Y   C H E C K

=================================================================================*/

--Check for Invalid Dates: Order Dates
SELECT NULLIF(sls_order_dt, 0) sls_order_dt

FROM bronze.crm_sales_details

WHERE
sls_order_dt < 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

--Check for Invalid Dates: Shipping Date
SELECT NULLIF(sls_ship_dt, 0) sls_ship_dt

FROM bronze.crm_sales_details

WHERE
sls_ship_dt < 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

--Check for Invalid Dates: Due Dates
SELECT NULLIF(sls_due_dt, 0) sls_due_dt

FROM bronze.crm_sales_details

WHERE
sls_due_dt < 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

--Find Invalid Date Orders: Order Dates must be lower than shipping date or due date
SELECT * 

FROM bronze.crm_sales_details

WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistency on sales, quantity, price
-- Sales = Quantity * Price
-- Values must not be NULL, zero or negative
SELECT DISTINCT
sls_sales AS old_sales
,sls_quantity
,sls_price AS old_price
,CASE
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
 END AS sls_sales
,CASE
	WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
 END AS sls_price

FROM bronze.crm_sales_details

WHERE
sls_sales <> sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

ORDER BY
sls_sales
,sls_quantity
,sls_price

/*=================================================================================

E X E C U T E   C H A N G E S

=================================================================================*/

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

FROM bronze.crm_sales_details

/*=================================================================================

	U P D A T E   M E T A D A T A   B A S E D   O N   C H A N G E S

=================================================================================*/
--sales_details
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
		CREATE TABLE silver.crm_sales_details
		(
			sls_ord_num 	 NVARCHAR(50)
			,sls_prd_key 	 NVARCHAR(50)
			,sls_cust_id 	 INT
			,sls_order_dt 	 DATE
			,sls_ship_dt 	 DATE
			,sls_due_dt 	 DATE
			,sls_sales 		 INT
			,sls_quantity 	 INT
			,sls_price 		 INT
			,dwh_create_date DATETIME2 DEFAULT GETDATE()
		);

/*=================================================================================

I N T E G R A T E   D A T A

=================================================================================*/

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

FROM bronze.crm_sales_details

/*=================================================================================

F I N A L   C H E C K I N G
							
=================================================================================*/
--EXPECTED: NO RESULT

--Find Invalid Date Orders: Order Dates must be lower than shipping date or due date
SELECT * 

FROM silver.crm_sales_details

WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistency on sales, quantity, price
-- Sales = Quantity * Price
-- Values must not be NULL, zero or negative
SELECT DISTINCT
sls_sales
,sls_quantity
,sls_price

FROM silver.crm_sales_details

WHERE
sls_sales <> sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

ORDER BY
sls_sales
,sls_quantity
,sls_price
