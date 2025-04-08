/*
=============================================================
Quality Checks
=============================================================
Script Purpose:
	This Script performs to check data quality before insert the data to silver layer

=============================================================
*/


-- Check duplicate values

USE DataWarehouse

select cst_id, count(*) as count1 
from bronze.crm_cust_info
group by cst_id
having count(*) > 1

select * from bronze.crm_cust_info where cst_firstname is null
select * from bronze.crm_cust_info where cst_id = 29466

SELECT * 
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info 
	)t WHERE flag_last = 1


-- Check for unwanted spaces
SELECT cst_firstname
from bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
from bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr from bronze.crm_cust_info 
SELECT DISTINCT cst_marital_status from bronze.crm_cust_info


/*
=====================================================
crm_cust_info
=====================================================
*/

select * from bronze.crm_prd_info

select prd_id, count(*) from bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null

--cehck unwanted spaces
select * from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

--Check for NULLS or negative numbers
SELECT * FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost is NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line FROM bronze.crm_prd_info

--Check dates
SELECT* FROM bronze.crm_prd_info
WHERE  prd_end_dt < prd_start_dt 


SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509') -- select two random key to fix the issue

select distinct id from bronze.erp_px_cat_g1v2


/*
=====================================================
crm_sales_details
=====================================================
*/

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num) or sls_prd_key != TRIM(sls_prd_key)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


--Check for Invalid Dates
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_date
FROM bronze.crm_sales_details
where sls_order_dt <= 0 or LEN(sls_order_dt) <8


SELECT 
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE)
END AS sls_order_dt
FROM bronze.crm_sales_details
where sls_order_dt <= 0 or LEN(sls_order_dt) <8

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt



--check negative or unneccery values in sales

SELECT 
sls_quantity,
sls_price,
sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0 OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL

SELECT 
sls_quantity,
sls_price,
sls_sales,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
	 ELSE sls_sales
END as sls_sales_new,

CASE WHEN sls_price IS NULL OR sls_price <= 0  THEN sls_sales / ABS(sls_quantity)
	 ELSE sls_price
END as sls_price_new

FROM bronze.crm_sales_details
WHERE sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0 OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL


SELECT 
* 
FROM silver.crm_sales_details 
WHERE sls_sales != sls_quantity * sls_price or sls_sales < 0 


/*
=====================================================
erp_cust_az12
=====================================================
*/

SELECT*FROM bronze.erp_cust_az12

SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN TRIM(SUBSTRING(cid,4,LEN(cid)))
	ELSE cid
END as cid_new
FROM bronze.erp_cust_az12



--Check all id exist in cust info_data set
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN TRIM(SUBSTRING(cid,4,LEN(cid)))
	ELSE cid
END as cid_new
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN TRIM(SUBSTRING(cid,4,LEN(cid)))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


--Check BirthDays out of dated

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--Check data Consistency
SELECT DISTINCT gen FROM bronze.erp_cust_az12

SELECT DISTINCT gen FROM silver.erp_cust_az12

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

/*
=====================================================
erp_cust_az12
=====================================================
*/

SELECT*FROM bronze.erp_loc_a101

SELECT DISTINCT cid FROM bronze.erp_loc_a101

SELECT
REPLACE(cid,'-','') as cid,
cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT cntry FROM bronze.erp_loc_a101


SELECT DISTINCT cid FROM silver.erp_loc_a101
SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry


/*
=====================================================
erp_px_cat_g1v2
=====================================================
*/

SELECT*FROM bronze.erp_px_cat_g1v2

--Check unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id)

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenace != TRIM(maintenace)

--Check Standardization & Consistency
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenace FROM bronze.erp_px_cat_g1v2



SELECT DISTINCT
id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT DISTINCT cat_id FROM silver.crm_prd_info)
