/*
===========================================================================
Stored Procedure: Load Bronze Layer (Bronze -> Silver)
===========================================================================
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema

	It performs the followig actions:
		- Truncate the silver tables
		- Insert the transformed data to the tables
How It Use:
	EXEC silver.load_silver

===========================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE()
	BEGIN TRY
		PRINT '=======================================';
		PRINT 'Loading silver Layer'
		PRINT '=======================================';

		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info

		PRINT '>> Inserting Data Into: silver.crm_cust_info'
		 INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
			 ELSE 'n/a'
		END as cst_marital_status, -- Normalize marital status values to readable format
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END as cst_gndr, -- Normalize gendar status values to readable format
		cst_create_date
		FROM (
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info 
			where cst_id is not NULL
			)t WHERE flag_last = 1 -- Select most recent record per customer

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ---------------';

		/*
		=====================================================
		crm_cust_info
		=====================================================
		*/

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- extract category id
		SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key, -- extract product key
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a'
		END as prd_line, --Normalize data to readble
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) as prd_end_dt --Calculate end date using next start datte - 1
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ---------------';



		/*
		=====================================================
		crm_sales_details
		=====================================================
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_quantity,
		sls_sales,
		sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt as VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt as VARCHAR) AS DATE)
		END AS sls_due_dt, --Cast values to correct one
		sls_quantity,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
			 ELSE sls_sales
		END as sls_sales, --Recalculated sales values if it missing or incorect

		CASE WHEN sls_price IS NULL OR sls_price <= 0  THEN sls_sales / ABS(sls_quantity)
			 ELSE sls_price
		END as sls_price  --Recalculated price values if it missing or incorect
		FROM bronze.crm_sales_details

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ---------------';


		/*
		=====================================================
		erp_cust_az12
		=====================================================
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN TRIM(SUBSTRING(cid,4,LEN(cid))) --Remove NAS for consistancy
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() then NULL
			 ELSE bdate
		END AS bdate, --Set future birthdate as null
		CASE WHEN TRIM(UPPER(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN TRIM(UPPER(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END as gen --Normalize data handle unknown 
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ---------------';


		/*
		=====================================================
		erp_cust_az12
		=====================================================
		*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT
		REPLACE(cid,'-','') as cid,
		CASE WHEN TRIM(UPPER(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
			 WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
			 WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
			 ELSE cntry
		END as cntry --Normalize and handle missing and null values
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ---------------';


		/*
		=====================================================
		erp_px_cat_g1v2
		=====================================================
		*/

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenace
		)
		SELECT 
		id,
		cat,
		subcat,
		maintenace
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ---------------';

		PRINT '===========================================';
		PRINT 'Loading Silver Layer is Completed'
		SET @batch_end_time = GETDATE();
		PRINT '>> Total Time Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '===========================================';
		

	END TRY
		BEGIN CATCH
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH
	
END;
