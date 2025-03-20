/*=================================================================

Q U A L I T Y   C H E C K 

=================================================================*/

--Check for duplicates and nulls
--Expectation: No Reuslt
SELECT
prd_id
,COUNT(*)

FROM bronze.crm_prd_info

GROUP BY prd_id

HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No Result
SELECT
prd_nm

FROM bronze.crm_prd_info

WHERE prd_nm <> TRIM(prd_nm)

--Check for NULLs or Negative Numbers
SELECT
prd_cost

FROM bronze.crm_prd_info

WHERE prd_cost IS NULL OR prd_cost < 0

--Quality Check: Replace Letters to Words
-- M = Mountain; R = Road; T = Touring; S = Other Sales
SELECT DISTINCT prd_line FROM bronze.crm_prd_info

--Quality Check: End Date shouldn't be earlier than Start Date
SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt

/*=================================================================

E X E C U T E   C H A N G E S

=================================================================*/

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

FROM bronze.crm_prd_info

--Quality Check: Filters unmatched data after applying transformation
--WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

--Quality Check: Filters unmatched data after applyang transformation
--WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details)

/*===========================================================================

	U P D A T E   M E T A D A T A   B A S E D   O N   C H A N G E S

===========================================================================*/

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
		CREATE TABLE silver.crm_prd_info
		(
			prd_id 			 INT
			,cat_id			 NVARCHAR(50)
			,prd_key 		 NVARCHAR(50)
			,prd_nm 		 NVARCHAR(50)
			,prd_cost 		 INT
			,prd_line 		 NVARCHAR(50)
			,prd_start_dt 	 DATE
			,prd_end_dt 	 DATE
			,dwh_create_date DATETIME2 DEFAULT GETDATE()
		);

/*=================================================================

I N T E G R A T E   D A T A

=================================================================*/

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

FROM bronze.crm_prd_info

--Quality Check: Filters unmatched data after applying transformation
--WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

--Quality Check: Filters unmatched data after applyang transformation
--WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details)

/*=================================================================

F I N A L   C H E C K I N G

=================================================================*/

--Check for duplicates and nulls
--Expectation: No Reuslt
SELECT
prd_id
,COUNT(*)

FROM silver.crm_prd_info

GROUP BY prd_id

HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No Result
SELECT
prd_nm

FROM silver.crm_prd_info

WHERE prd_nm <> TRIM(prd_nm)

--Check for NULLs or Negative Numbers
SELECT
prd_cost

FROM silver.crm_prd_info

WHERE prd_cost IS NULL OR prd_cost < 0

--Quality Check: Replace Letters to Words
-- M = Mountain; R = Road; T = Touring; S = Other Sales
SELECT DISTINCT prd_line FROM silver.crm_prd_info

--Quality Check: End Date shouldn't be earlier than Start Date
SELECT * FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt

SELECT * FROM silver.crm_prd_info
