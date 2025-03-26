/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*=======================================================================

					C H E C K   D A T A   I N T E G R I T Y

=======================================================================*/
--Check for duplicates for Gold Layer Customer Table
--Expected Result: No result shown
SELECT
ci.cst_id AS customer_id
,ci.cst_key AS customer_key
,ci.cst_firstname AS first_name
,ci.cst_lastname AS last_name
,ci.cst_marital_status AS marital_status
--Use crm_cust_info as master data and use values there for duplicate columns
,CASE
	WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
END AS gender
,ci.cst_create_date AS create_date
,ca.BDATE AS birthdate
,cl.CNTRY AS country

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 cl ON ci.cst_key = cl.CID

--Filter Out Historical Data
--Expected Result: Product End Date should show NULLs, this will indicate that it is the latest product information
SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key
,pn.prd_id AS product_id
,pn.prd_key AS product_number
,pn.prd_nm AS product_name
,pn.cat_id AS category_id
,pc.CAT AS category
,pc.SUBCAT AS subcategory
,pc.MAINTENANCE AS maintenance
,pn.prd_cost AS product_cost
,pn.prd_line AS product_line
,pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.ID

WHERE prd_end_dt IS NULL --Filter out historical data

-- Integrate Surrogate keys from Gold Layer Dimension tables
-- Input Surrogate Keys created in Customer and Product Tables
SELECT
sd.sls_ord_num AS order_number
,pr.product_key AS product_key -- Surrogate Key from Product Table
,cu.customer_key AS customer_key -- Surrogate Key from Customer Table
,sd.sls_order_dt AS order_date
,sd.sls_ship_dt AS ship_date
,sd.sls_due_dt AS due_date
,sd.sls_sales AS sales_amount
,sd.sls_quantity AS quantity
,sd.sls_price AS price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
