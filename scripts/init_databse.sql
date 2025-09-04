/*
============================================================================
Creating Database with Shemas
============================================================================
Script Purpose:
This script creates, after checking and dropping if it already exists, a new database named DataWarehouse. Additionally, this
script creates three disctincts schemas (bronze, silver and gold) in the DataWarehouse database

Warning:
The script will erase an existing database named DataWarehouse.
Backup relevant data in existing database with the same name and relevant data if required

==============================================================================


-- Creating Database Warehouse
USE master;
GO 

-- Drops DataWarehouse Database if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE WareHouse;
END
GO

--Creates Database named DataWarehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Creates different schemas in the DataWarehouse 
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
