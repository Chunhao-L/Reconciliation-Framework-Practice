/*
=====================================================================================
Purpose:
--------
This script sets up the foundational structure for a data warehouse or data lakehouse
environment in SQL Server by performing the following steps:

1. Creates a new database named 'TheDataGuyzPractice'.
2. Switches the context to the new database.
3. Creates three schemas:
   - RAW_LAYER: Used to store raw, unprocessed data ingested from source systems.
   - STG_LAYER: Staging area for cleaning, deduplication, and transformation processes.
   - AUDITING: Used to store metadata, logs, or audit trails to monitor data operations.

How to Use:
-----------
Run this script in SQL Server Management Studio (SSMS) or another SQL interface connected
to a SQL Server instance. It should be executed by a user with permissions to create
databases and schemas.

Preconditions:
--------------
- The script assumes no existing database with the same name ('TheDataGuyzPractice').
  If one exists, either drop it or use a different name.
=====================================================================================
*/

-- Create a new database named TheDataGuyzPractice
CREATE DATABASE TheDataGuyzPractice;
GO  -- Separates the batch so that the DB can be referenced in the next step

-- Change context to the newly created database
USE TheDataGuyzPractice;
GO  -- Required to ensure following objects are created in the correct DB

-- Create schema to store raw/unprocessed data ingested from source systems
CREATE SCHEMA RAW_LAYER;
GO

-- Create schema to hold staging (intermediate) tables used for data transformations
CREATE SCHEMA STG_LAYER;
GO

-- Create schema for storing audit logs, metadata, and job tracking information
CREATE SCHEMA AUDITING;
GO
