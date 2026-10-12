-- DEV ONLY: reset this project's derived tables before the UCI reload.
-- DROP is DDL. Source files, code, schemas, and Volumes are preserved.
-- Stop scheduled jobs and concurrent notebook runs before execution.
USE CATALOG workspace;
USE SCHEMA retail_dev_schema;

-- Gold and Silver from the previous implementations.
DROP TABLE IF EXISTS workspace.retail_dev_schema.gold_daily_sales;
DROP TABLE IF EXISTS workspace.retail_dev_schema.gold_product_sales;
DROP TABLE IF EXISTS workspace.retail_dev_schema.silver_orders;
DROP TABLE IF EXISTS workspace.retail_dev_schema.silver_order_items;
DROP TABLE IF EXISTS workspace.retail_dev_schema.silver_products;
DROP TABLE IF EXISTS workspace.retail_dev_schema.silver_customers_history;
DROP TABLE IF EXISTS workspace.retail_dev_schema.silver_customers_current;
DROP TABLE IF EXISTS workspace.retail_dev_schema.silver_users;

-- Bronze from retail and the optional NYC implementation.
DROP TABLE IF EXISTS workspace.retail_dev_schema.bronze_transactions;
DROP TABLE IF EXISTS workspace.retail_dev_schema.bronze_users;
DROP TABLE IF EXISTS workspace.retail_dev_schema.bronze_products;
DROP TABLE IF EXISTS workspace.retail_dev_schema.bronze_carts;
DROP TABLE IF EXISTS workspace.retail_dev_schema.bronze_service_requests;

-- Reset processing ledgers together with their target data.
DROP TABLE IF EXISTS workspace.retail_dev_schema.retail_monitoring;
DROP TABLE IF EXISTS workspace.retail_dev_schema.retail_quarantine;
DROP TABLE IF EXISTS workspace.retail_dev_schema.pipeline_monitoring;
DROP TABLE IF EXISTS workspace.retail_dev_schema.quarantine_records;

-- Inventory remaining objects; unrelated tables are not removed.
SHOW TABLES IN workspace.retail_dev_schema;
