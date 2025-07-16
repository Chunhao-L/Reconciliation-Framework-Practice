/*
==================================================================================================
Purpose:
--------
This script prepares the **Staging Layer (STG_LAYER)** of the data pipeline by creating two
staging tables: `CUSTOMERS` and `PRODUCTS`.

These tables are intended to hold cleaned and flattened data extracted from the raw JSON blobs
in the `RAW_LAYER.BATCH_EXTRACT` table. Each record represents a single entity instance
(customer or product), enriched with batch metadata.

How to Use:
-----------
- Run this after ingesting data into RAW_LAYER.
- The tables are safe to be dropped and recreated on every load if needed.
- Typically, a transformation process will extract entity JSON from RAW_LAYER and load
  structured rows here via parsing logic or stored procedures.

Preconditions:
--------------
- Database `TheDataGuyzPractice` and schema `STG_LAYER` must already exist.
- SQL Server 2016+ recommended for JSON parsing features (if used).
==================================================================================================
*/

-- Switch to the working database
USE TheDataGuyzPractice;
GO

-- Drop existing CUSTOMERS table in staging layer if it exists (to reset structure/data)
IF OBJECT_ID('STG_LAYER.CUSTOMERS', 'U') IS NOT NULL
    DROP TABLE STG_LAYER.CUSTOMERS;
GO

-- Create a staging table for customer entities
CREATE TABLE STG_LAYER.CUSTOMERS (
    batch_id INT,             -- Identifier for the source batch (from RAW_LAYER)
    entity_event_id INT,      -- Unique identifier for the event or record in the staging context
    customer_id INT,          -- Business key for the customer (entity_id from RAW_LAYER)
    fname NVARCHAR(50),       -- Customer first name
    lname NVARCHAR(50),       -- Customer last name
    gender NVARCHAR(1)        -- Gender, e.g., 'M' or 'F'
);

-- Drop existing PRODUCTS table in staging layer if it exists
IF OBJECT_ID('STG_LAYER.PRODUCTS', 'U') IS NOT NULL
    DROP TABLE STG_LAYER.PRODUCTS;
GO

-- Create a staging table for product entities
CREATE TABLE STG_LAYER.PRODUCTS (
    batch_id INT,             -- Identifier for the batch the product came from
    entity_event_id INT,      -- Unique row ID or surrogate key for internal tracking
    product_id INT,           -- Business key for the product (entity_id from RAW_LAYER)
    product_name NVARCHAR(50),-- Name of the product
    unit_price INT            -- Price per unit (assumed to be integer for simplicity)
);


DELETE FROM STG_LAYER.CUSTOMERS WHERE entity_event_id = 10;
select * from STG_LAYER.CUSTOMERS;
select * from STG_LAYER.PRODUCTS;