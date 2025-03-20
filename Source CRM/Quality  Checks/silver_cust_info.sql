/*===============================================================

Q U A L I T Y  C H E C K 
							
===============================================================*/

--Check for nulls or duplicates in the primary key
--Expectation: No result
SELECT
cst_id
,COUNT(*) dupes

FROM bronze.crm_cust_info

GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Flagging the first entry for the duplicates
SELECT *

FROM
(
SELECT
*
,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last

FROM bronze.crm_cust_info
) a

WHERE flag_last = 1

--Check for unwanted spaces
--Expectation: No result
SELECT cst_firstname FROM bronze.crm_cust_info WHERE cst_firstname <> TRIM(cst_firstname)
SELECT cst_lastname FROM bronze.crm_cust_info WHERE cst_lastname <> TRIM(cst_lastname)
SELECT cst_gndr FROM bronze.crm_cust_info WHERE cst_gndr <> TRIM(cst_gndr)

--Data Standardization and consistency
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info

/*===============================================================

E X E C U T E   C H A N G E S
							
===============================================================*/

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

FROM bronze.crm_cust_info

/*===============================================================

I N T E G R A T E   D A T A
					
===============================================================*/

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
) a

/*===============================================================

F I N A L   C H E C K I N G
					
===============================================================*/

--Check for nulls or duplicates in the primary key
--Expectation: No result
SELECT
cst_id
,COUNT(*) dupes

FROM silver.crm_cust_info

GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check for unwanted spaces
--Expectation: No result
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname <> TRIM(cst_firstname)
SELECT cst_lastname FROM silver.crm_cust_info WHERE cst_lastname <> TRIM(cst_lastname)
SELECT cst_gndr FROM silver.crm_cust_info WHERE cst_gndr <> TRIM(cst_gndr)

--Data Standardization and consistency
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info
