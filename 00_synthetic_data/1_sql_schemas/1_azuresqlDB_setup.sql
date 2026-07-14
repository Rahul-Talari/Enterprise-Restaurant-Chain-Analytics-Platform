"*
Get details from Connection String in Azure Portal for SQL Database and update the below details accordingly to connect the Database.

DB Name:      <Enter your Database Name>
server:       <Enter your Server Name>
username:     <Enter your Username>
password:     <Enter your Password>
JDBC_String:  jdbc:sqlserver://<Enter your Server Name>.database.windows.net:1433;database=<Enter your Database Name>;user=<Enter your Username>;password=<Enter your Password>;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
*"

-- =====================================================
-- RESTAURANTS
-- =====================================================

CREATE TABLE DB_NAME.SCHEMA_NAME.restaurants (

    restaurant_id VARCHAR(256),

    name VARCHAR(256),
    city VARCHAR(256),
    country VARCHAR(256),
    address VARCHAR(MAX),
    opening_date DATE,
    phone VARCHAR(256),
    last_updated DATETIME2,

    CONSTRAINT PK_Restaurants
        PRIMARY KEY (restaurant_id)
);

-- =====================================================
-- CUSTOMERS
-- =====================================================

CREATE TABLE DB_NAME.SCHEMA_NAME.customers (

    customer_id VARCHAR(256),

    name VARCHAR(256),
    email VARCHAR(256),
    phone VARCHAR(256),
    city VARCHAR(256),
    join_date DATE,
    last_updated DATETIME2,

    CONSTRAINT PK_Customers
        PRIMARY KEY (customer_id)
);

-- =====================================================
-- MENU ITEMS
-- =====================================================

CREATE TABLE DB_NAME.SCHEMA_NAME.menu_items (

    restaurant_id VARCHAR(256),
    item_id VARCHAR(256),

    name VARCHAR(256),
    category VARCHAR(256),
    price DECIMAL(10,2),
    ingredients VARCHAR(256),
    is_vegetarian BIT,
    spice_level VARCHAR(256),
    last_updated DATETIME2,

    CONSTRAINT PK_Menu_Items
        PRIMARY KEY (restaurant_id, item_id),

    CONSTRAINT FK_MenuItems_Restaurants
        FOREIGN KEY (restaurant_id)
        REFERENCES DB_NAME.SCHEMA_NAME.restaurants(restaurant_id),

    CONSTRAINT CK_MenuItems_SpiceLevel
        CHECK (spice_level IN ('None','Mild','Medium','Hot'))
);

-- =====================================================
-- HISTORICAL ORDERS
-- =====================================================

CREATE TABLE DB_NAME.SCHEMA_NAME.historical_orders (

    order_id VARCHAR(256),

    order_timestamp DATETIME2,

    restaurant_id VARCHAR(256),
    customer_id VARCHAR(256),

    order_type VARCHAR(256),
    items NVARCHAR(MAX),      -- JSON array
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(256),
    order_status VARCHAR(256),
    last_updated DATETIME2,

    CONSTRAINT PK_Historical_Orders
        PRIMARY KEY (order_id),

    CONSTRAINT FK_Orders_Restaurants
        FOREIGN KEY (restaurant_id)
        REFERENCES DB_NAME.SCHEMA_NAME.restaurants(restaurant_id),

    CONSTRAINT FK_Orders_Customers
        FOREIGN KEY (customer_id)
        REFERENCES DB_NAME.SCHEMA_NAME.customers(customer_id),

    CONSTRAINT CK_Order_Type
          CHECK (order_type IN ('dine_in','takeaway','delivery'))

    CONSTRAINT CK_Order_Status
         CHECK (order_status IN ('received','preparing','ready','delivered','completed'))
);

-- =====================================================
-- REVIEWS
-- =====================================================

CREATE TABLE DB_NAME.SCHEMA_NAME.reviews (

    review_id VARCHAR(256),

    order_id VARCHAR(256),
    customer_id VARCHAR(256),
    restaurant_id VARCHAR(256),

    review_text NVARCHAR(MAX),
    rating INT,
    review_timestamp DATETIME2,
    last_updated DATETIME2,
    CONSTRAINT PK_Reviews
        PRIMARY KEY (review_id),

    CONSTRAINT FK_Reviews_Orders
        FOREIGN KEY (order_id)
        REFERENCES DB_NAME.SCHEMA_NAME.historical_orders(order_id),

    CONSTRAINT FK_Reviews_Customers
        FOREIGN KEY (customer_id)
        REFERENCES DB_NAME.SCHEMA_NAME.customers(customer_id),

    CONSTRAINT FK_Reviews_Restaurants
        FOREIGN KEY (restaurant_id)
        REFERENCES DB_NAME.SCHEMA_NAME.restaurants(restaurant_id),

    CONSTRAINT UQ_Reviews_Order
        UNIQUE (order_id),

    CONSTRAINT CK_Reviews_Rating
        CHECK (rating BETWEEN 1 AND 5)
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX IX_Restaurants_LastUpdated
ON DB_NAME.SCHEMA_NAME.restaurants(last_updated);

CREATE INDEX IX_Customers_LastUpdated
ON DB_NAME.SCHEMA_NAME.customers(last_updated);

CREATE INDEX IX_MenuItems_LastUpdated
ON DB_NAME.SCHEMA_NAME.menu_items(last_updated);

CREATE INDEX IX_HistoricalOrders_LastUpdated
ON DB_NAME.SCHEMA_NAME.historical_orders(last_updated);

CREATE INDEX IX_Reviews_LastUpdated
ON DB_NAME.SCHEMA_NAME.reviews(last_updated);


-- Individual table record counts:
select count(*) from DB_NAME.SCHEMA_NAME.customers;
select count(*) from DB_NAME.SCHEMA_NAME.historical_orders;
select count(*) from DB_NAME.SCHEMA_NAME.menu_items;
select count(*) from DB_NAME.SCHEMA_NAME.restaurants;
select count(*) from DB_NAME.SCHEMA_NAME.reviews;

-- All table record counts in a single query:
SELECT 'customers' AS table_name, COUNT(*) AS record_count FROM DB_NAME.SCHEMA_NAME.customers
UNION ALL
SELECT 'historical_orders', COUNT(*) FROM DB_NAME.SCHEMA_NAME.historical_orders
UNION ALL
SELECT 'menu_items', COUNT(*) FROM DB_NAME.SCHEMA_NAME.menu_items
UNION ALL
SELECT 'restaurants', COUNT(*) FROM DB_NAME.SCHEMA_NAME.restaurants
UNION ALL
SELECT 'reviews', COUNT(*) FROM DB_NAME.SCHEMA_NAME.reviews;


"*
To synchronize Azure SQL Database with Databricks Lakeflow Connect for incremental data ingestion, enable either of the following:

Option 1: Change Data Capture (CDC)
 Purpose:
   - Captures WHAT changed.
   - Stores detailed change history including old and new values.
   - Best for auditing and historical tracking.

Option 2: Change Tracking (CT)
Purpose:
  - Captures WHICH rows changed.
  - Stores only row identifiers and version information.
  - Latest row values are retrieved from the source table.
  - Best for incremental ETL and data synchronization.

*"

-- ==============================================================================
-- LAKEFLOW CONNECT SETUP (SQL SERVER)
-- Purpose: Prepare Azure SQL DB  for Databricks Lakeflow Connect incremental data ingestion.
-- ==============================================================================


-- ==============================================================================
-- STEP 1: Download and Execute Lakeflow SQL Utility Script
-- ==============================================================================
-- Utility Script:
-- https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/sql-server-utility

--
-- This script creates Lakeflow helper stored procedures in dbo schema:
--   1. lakeflowFixPermissions
--   2. lakeflowSetupChangeTracking
--   3. lakeflowSetupChangeDataCapture
--
-- These procedures automate permission assignment and incremental ingestion setup.

SELECT dbo.lakeflowUtilityVersion_1_5() AS UtilityVersion;
SELECT dbo.lakeflowDetectPlatform() AS Platform;

-- ==============================================================================
-- STEP 2: Enable CT (for tables with primary keys) & Enable CDC (for tables without primary keys)
-- ==============================================================================

-- Enable Change Tracking (CT) for Master Data Tables
EXEC dbo.lakeflowSetupChangeTracking
    @Tables = 'dbo.restaurants,dbo.customers,dbo.menu_items',
    @User = 'sqladmin',                                          -- Database user granted read access. Lakeflow uses this user to read changes from the source tables.
    @Retention = '2 DAYS';                                       -- Retains change tracking metadata for 2 days. Lakeflow must read the changes before this period expires.
GO

-- Enable Change Data Capture (CDC) for Transactional Tables
EXEC dbo.lakeflowSetupChangeDataCapture
    @Tables = 'dbo.historical_orders,dbo.reviews',
    @User = 'sqladmin';                                          -- Database user granted read access. Lakeflow uses this user to read change data from the source tables.
GO

-- ==============================================================================
-- STEP 3: Grant required permissions to Lakeflow user
-- ==============================================================================
EXEC dbo.lakeflowFixPermissions
    @User = 'sqladmin',      -- Database user used by Lakeflow to connect to the source database.
    @Tables = 'ALL';         -- Grants the required SELECT permissions on all source tables.
GO

-- ==============================================================================
-- STEP 4: Verify setup
-- ==============================================================================

-- =====================================================
-- Verify Change Tracking (CT) Configuration
-- =====================================================

-- Verify Change Tracking is enabled for the current database
SELECT
    d.name AS DatabaseName,
    ctd.is_auto_cleanup_on,
    ctd.retention_period,
    ctd.retention_period_units_desc
FROM sys.change_tracking_databases ctd
INNER JOIN sys.databases d
    ON ctd.database_id = d.database_id
WHERE d.name = DB_NAME();

-- Verify tables with Change Tracking enabled
SELECT
    SCHEMA_NAME(t.schema_id) + '.' + t.name AS TableName,
    ct.is_track_columns_updated_on,
    ct.begin_version,
    ct.cleanup_version
FROM sys.change_tracking_tables ct
INNER JOIN sys.tables t
    ON ct.object_id = t.object_id;

-- =====================================================
-- Verify Change Data Capture (CDC) Configuration
-- =====================================================

-- Verify CDC is enabled for the current database
SELECT
    DB_NAME() AS DatabaseName,
    is_cdc_enabled
FROM sys.databases
WHERE database_id = DB_ID();

-- Verify tables with CDC enabled
SELECT
    SCHEMA_NAME(t.schema_id) + '.' + t.name AS TableName,
    ct.capture_instance,
    ct.start_lsn,
    ct.create_date
FROM cdc.change_tables ct
INNER JOIN sys.tables t
    ON ct.source_object_id = t.object_id;


-- ============================================
-- CDC Commands
-- ============================================
update customers
set city='Abu Dhabi'
where customer_id='CUST-10000';

insert into dbo.menu_items (restaurant_id, item_id, name, category, price, ingredients, is_vegetarian, spice_level)
values ('REST-AUH-001','ITEM-999','Samosa (2 pcs)','Starter',18.49,'Potato, Peas, Spices, Pastry',1,'Medium');