/*
==============================================
Script Purpose:
	This script helps to check silver layer data and do some transformation and 
	joining related tables
==============================================
*/


/*
==============================================
dim_customer
==============================================
*/

SELECT cst_id, COUNT(*) FROM(
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON		  ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) >1


SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON		  ci.cst_key = la.cid

/*
==============================================
dim_products
==============================================
*/

SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenace
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data

SELECT prd_key, COUNT(*) FROM(
SELECT
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.maintenace
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data
)t GROUP BY prd_key
HAVING COUNT(*) >2


SELECT
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenace,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data
