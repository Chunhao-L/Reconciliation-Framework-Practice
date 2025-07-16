/*
==================================================================================================
Purpose:
--------
This script is designed to set up and populate the `RAW_LAYER` schema with two base tables:
1. `BATCH_EXTRACT`: Stores full JSON payloads per batch (e.g., as received from a source system).
2. `ENTITY_EXTRACT`: Stores flattened, entity-level rows (to be populated later via transformation).

The script:
- Drops existing tables if they exist (for rerun convenience).
- Creates new tables to hold incoming raw JSON data.
- Inserts multiple sample batch records with JSON arrays containing customer/product data.

How to Use:
-----------
- Execute in SQL Server Management Studio (SSMS) or equivalent connected to a SQL Server instance.
- Assumes `TheDataGuyzPractice` database and `RAW_LAYER` schema already exist.
- Can be extended to transform the JSON in `BATCH_EXTRACT` into structured rows in `ENTITY_EXTRACT`.

Preconditions:
--------------
- SQL Server 2016 or later (for NVARCHAR(MAX) and JSON compatibility if transformations are added).
==================================================================================================
*/

-- Switch context to the target database
USE TheDataGuyzPractice;
GO

-- Drop table if it already exists to avoid errors on creation (safe for re-runs)
IF OBJECT_ID('RAW_LAYER.BATCH_EXTRACT', 'U') IS NOT NULL
    DROP TABLE RAW_LAYER.BATCH_EXTRACT;
GO

-- Create a table to hold entire raw JSON payloads ingested per batch
CREATE TABLE RAW_LAYER.BATCH_EXTRACT (
    batch_id INT,                      -- Unique identifier for the batch
    extraction_date DATETIME,         -- Timestamp when the batch was ingested
    json_data NVARCHAR(MAX)           -- Full JSON string with all batch data
);

-- Drop the ENTITY_EXTRACT table if it exists
IF OBJECT_ID('RAW_LAYER.ENTITY_EXTRACT', 'U') IS NOT NULL
    DROP TABLE RAW_LAYER.ENTITY_EXTRACT;
GO

-- Create a table to hold flattened entities (to be populated post JSON parsing)
CREATE TABLE RAW_LAYER.ENTITY_EXTRACT (
    batch_id INT,                     -- ID of the batch the entity belongs to
    entity_event_id INT,             -- Surrogate key or row ID (could be generated later)
    entity_id INT,                   -- ID of the specific entity (customer/product)
    extraction_date DATETIME,        -- Timestamp of extraction (redundant but useful)
    entity_type NVARCHAR(50),        -- e.g., 'customer' or 'product'
    entity_json_data NVARCHAR(MAX)   -- JSON snippet for the individual entity
);

-- Truncate any existing data in BATCH_EXTRACT before inserting new records
TRUNCATE TABLE RAW_LAYER.BATCH_EXTRACT;

-- Insert sample batch data as JSON payloads
INSERT INTO RAW_LAYER.BATCH_EXTRACT (batch_id, extraction_date, json_data)
VALUES 
-- Batch 1: Contains 4 entities
(1, '2025-07-07 10:00:01', 
N'{
  "batch_id": 1,
  "batch_data": [
    { "entity_id": 1001, "entity_type": "customer", "entity": { "first_name": "Alice", "last_name": "Brown", "gender": "F" } },
    { "entity_id": 1005, "entity_type": "customer", "entity": { "first_name": "Ethan", "last_name": "Wong", "gender": "M" } },
    { "entity_id": 2001, "entity_type": "product",  "entity": { "product_name": "apple",  "unit_price": 2 } },
    { "entity_id": 2002, "entity_type": "product",  "entity": { "product_name": "banana", "unit_price": 1 } }
  ]
}'),

-- Batch 2: Contains 4 entities, including duplicates like entity_id 2001
(2, '2025-07-07 10:00:02', 
N'{
  "batch_id": 2,
  "batch_data": [
    { "entity_id": 1002, "entity_type": "customer", "entity": { "first_name": "Bob", "last_name": "Smith", "gender": "M" } },
    { "entity_id": 2003, "entity_type": "product",  "entity": { "product_name": "orange", "unit_price": 3 } },
    { "entity_id": 2001, "entity_type": "product",  "entity": { "product_name": "apple",  "unit_price": 2 } },
    { "entity_id": 2004, "entity_type": "product",  "entity": { "product_name": "peach",  "unit_price": 4 } }
  ]
}'),

-- Batch 3: 3 entities (2 customers, 1 product)
(3, '2025-07-07 10:00:03', 
N'{
  "batch_id": 3,
  "batch_data": [
    { "entity_id": 1003, "entity_type": "customer", "entity": { "first_name": "Cathy", "last_name": "Lee", "gender": "F" } },
    { "entity_id": 1006, "entity_type": "customer", "entity": { "first_name": "Nathan", "last_name": "Chan", "gender": "M" } },
    { "entity_id": 2002, "entity_type": "product",  "entity": { "product_name": "banana", "unit_price": 1 } }
  ]
}');

-- Insert a separate 4th batch to demonstrate appending more data later
INSERT INTO RAW_LAYER.BATCH_EXTRACT (batch_id, extraction_date, json_data)
VALUES 
-- Batch 4: Contains 6 entities
(4, '2025-07-07 10:00:04', 
N'{
  "batch_id": 4,
  "batch_data": [
    { "entity_id": 1004, "entity_type": "customer", "entity": { "first_name": "David", "last_name": "Chen", "gender": "M" } },
    { "entity_id": 2001, "entity_type": "product",  "entity": { "product_name": "apple",  "unit_price": 2 } },
    { "entity_id": 2002, "entity_type": "product",  "entity": { "product_name": "banana", "unit_price": 1 } },
    { "entity_id": 2005, "entity_type": "product",  "entity": { "product_name": "grape",  "unit_price": 5 } },
    { "entity_id": 2003, "entity_type": "product",  "entity": { "product_name": "orange", "unit_price": 3 } },
    { "entity_id": 2004, "entity_type": "product",  "entity": { "product_name": "peach",  "unit_price": 4 } }
  ]
}');


select * from RAW_LAYER.BATCH_EXTRACT;
select * from RAW_LAYER.ENTITY_EXTRACT;
