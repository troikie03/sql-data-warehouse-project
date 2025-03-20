/*=================================================================

Q U A L I T Y   C H E C K

=================================================================*/
--Match the format of the customer ID: Remove the dash
SELECT REPLACE(CID, '-', '') AS CID FROM bronze.erp_loc_a101

--Match the country abbreviations to actual country names
SELECT DISTINCT CNTRY FROM bronze.erp_loc_a101

/*=================================================================

I N T E G R A T E   D A T A

=================================================================*/

INSERT INTO silver.erp_loc_a101 (CID,CNTRY)

SELECT
REPLACE(CID, '-', '') CID
,CASE
	WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
	WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
	WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = ' ' THEN 'n/a'
	ELSE TRIM(CNTRY)
END CNTRY

FROM bronze.erp_loc_a101

/*=================================================================

F I N A L   C H E C K I N G

=================================================================*/
--Match the format of the customer ID: Remove the dash
--Expected Result: No dash on Customer ID
SELECT REPLACE(CID, '-', '') AS CID FROM silver.erp_loc_a101 WHERE CID LIKE '%-%'

--Match the country abbreviations to actual country names
-- Country Names are shown not abbreviations, and NULLs are tranformed to n/a
SELECT DISTINCT CNTRY FROM silver.erp_loc_a101

