-- =============================================
-- PostgreSQL
-- =============================================

-- Step 1: Terminate active connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'DataWarehouse'
  AND pid <> pg_backend_pid();

-- Step 2: Drop & recreate database
DROP DATABASE IF EXISTS "DataWareHouse";
CREATE DATABASE "DataWareHouse";

-- Step 3: Connect to the new database
\connect DataWarehouse;

-- Step 4: Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
