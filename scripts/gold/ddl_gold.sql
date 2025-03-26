/*==============================================================

                    DDL SCRIPT: Create Gold Views

==============================================================*/

/*--------------------------------------------------------------
Script Purpose:
This script creates views for the gold layer in the data
warehouse. The gold layer represents the final dimensions and
fact table (Star Schema).

Each view performs transformations and combines data from the
silver layer to produce a clean, enriched, and business-ready
dataset

USAGE: These views can be queried directly for analytics and
reporting.
--------------------------------------------------------------*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS

SELECT
ROW_NUMBER() OVER(ORDER BY cst_key) AS customer_key
ci.cst_id				AS customer_id
,ci.cst_key				AS customer_number
,ci.cst_firstname		AS first_name
,ci.cst_lastname		AS last_name
,cl.CNTRY				AS country
,ci.cst_marital_status  AS marital_status
--Use crm_cust_info as master data and use values there for duplicate columns
,CASE
	WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
END						AS gender
,ca.BDATE				AS birthdate
,ci.cst_create_date		AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 cl  ON ci.cst_key = cl.CID
GO

-- =============================================================================
-- Create Fact Table: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS

SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key
,pn.prd_id		 AS product_id
,pn.prd_key		 AS product_number
,pn.prd_nm		 AS product_name
,pn.cat_id		 AS category_id
,pc.CAT			 AS category
,pc.SUBCAT		 AS subcategory
,pc.MAINTENANCE  AS maintenance
,pn.prd_cost	 AS product_cost
,pn.prd_line	 AS product_line
,pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.ID

WHERE prd_end_dt IS NULL --Filter out historical data
GO
-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

--Create Views for Fact Table
CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num	 AS order_number
,pr.product_key  AS product_key
,cu.customer_key AS customer_key
,sd.sls_order_dt AS order_date
,sd.sls_ship_dt  AS ship_date
,sd.sls_due_dt	 AS due_date
,sd.sls_sales	 AS sales_amount
,sd.sls_quantity AS quantity
,sd.sls_price	 AS price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr  ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
GO
