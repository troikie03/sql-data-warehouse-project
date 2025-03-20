/*=================================================================

Q U A L I T Y   C H E C K

=================================================================*/

--Remove Unwanted Values: 'NAS' on the CID
SELECT
CASE
	WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
	ELSE CID
END AS CID 
 
 FROM bronze.erp_cust_az12
 
 --Remove Invalid Dates: Check for dates that surpasses the present
SELECT
CASE
	WHEN BDATE > GETDATE() THEN NULL
	ELSE BDATE
END AS BEDATE 

FROM bronze.erp_cust_az12
 
 --Data Normalization: Format the values of the gender for uniformity
 
CASE
	WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
	ELSE 'n/a'
END AS GEN

FROM bronze.erp_cust_az12

/*=================================================================

E X E C U T E   C H A N G E S

=================================================================*/

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

FROM bronze.erp_cust_az12

/*=================================================================

I N T E G R A T E   D A T A

=================================================================*/
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

FROM bronze.erp_cust_az12

/*=================================================================

F I N A L   C H E C K I N G

=================================================================*/

-- Removed Unwanted Value 'NAS%'
-- Expected Result: CID starts with AW
SELECT CID FROM silver.erp_cust_az12 WHERE CID LIKE 'NAS%'

-- Removed dates surpassing present dates
-- Expected Result: Future dates removed
SELECT BDATE FROM silver.erp_cust_az12 WHERE BDATE > GETDATE()

-- Value Formatting
-- Expected Result: Genders should only show the ff: Male, Female, n/a
SELECT DISTINCT GEN FROM silver.erp_cust_az12
