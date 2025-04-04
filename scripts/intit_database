USE master;
GO

/*
===============================================
Create Database and Schemas
===============================================

Script Purpose
	This script create new database called 'DataWarehouse'. Firstly checked if databse exist called 'DataWarehouse' if it exist delete
	permenetly and create new one, after that create schemas three new schemas called 'bronze', 'silver', 'bronze'

WARNING:
	Running this script delete exist 'DataWarehouse' database permently
*/



IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO


-- Create Database 'DataWarehouse'


CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

--Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
