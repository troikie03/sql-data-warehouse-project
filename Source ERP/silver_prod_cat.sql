/*=================================================================

Q U A L I T Y   C H E C K

=================================================================*/
-- Check for Unwanted Spaces
SELECT CAT FROM bronze.erp_px_cat_g1v2
WHERE CAT <> TRIM(CAT) OR SUBCAT <> TRIM(SUBCAT) OR MAINTENANCE <> TRIM(MAINTENANCE)

-- Data Standardization & Consistency
SELECT DISTINCT CAT FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT SUBCAT FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT MAINTENANCE FROM bronze.erp_px_cat_g1v2


/*=================================================================

I N T E G R A T E   D A T A

=================================================================*/

INSERT INTO silver.erp_px_cat_g1v2 (CID, CAT, SUBCAT, MAINTENANCE)

SELECT * FROM bronze.erp_px_cat_g1v2
