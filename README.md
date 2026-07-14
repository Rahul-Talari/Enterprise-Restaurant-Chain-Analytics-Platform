# Enterprise Restaurant Chain Analytics Lakehouse Platform

---

# Business Process Model

<p align="center">
  <img src="diagrams/1_Business_ProcessModel.png" alt="Business Process Model" width="900"/>
</p>

The Business Process Model provides a high-level view of how the restaurant business operates. It focuses on the interactions between customers, restaurant branches, orders, menu items, and reviews without considering database implementation details.

Restaurant Chain
```
    └── Operates multiple Restaurant Branches.
```

Restaurant Branch
```
    ├── Offers multiple Menu Items          : Each Menu Item belongs to one Menu Category.
    ├── Receives multiple Customer Orders.  : After an order is placed, the restaurant branch prepares and fulfills it.
    └── Receives multiple Customer Reviews.
```

Customer
```
    ├── Registers with the restaurant chain.
    ├── Places multiple Orders from any restaurant branch.
    └── Writes multiple Customer Reviews.
```

Customer Order
```
    ├── Contains one or more Menu Items.                  Order Type   : Dine-in, Takeaway, Delivery
    ├── Progresses through Order Statuses.                Ex           : Received, Preparing, Ready, Delivered, Completed
    ├── Is associated with one Payment.                   Payment Types: Cash, Card, Digital Wallet
    └── Can receive one Customer Review after completion.
```

---

# Conceptual Data Model

<p align="center">
  <img src="diagrams/2_Conceptual_DataModel.png" alt="Conceptual Data Model" width="850"/>
</p>

The Conceptual Data Model identifies the primary business entities and their relationships. It represents the business domain at a high level without including implementation details such as primary keys, foreign keys, or data types.

## Entity Relationships

| Entity | Related Entity | Cardinality | Business Rule |
|---------|----------------|-------------|---------------|
| Restaurant | Menu Item | 1 : Many | A restaurant offers multiple menu items. |
| Restaurant | Historical Order | 1 : Many | A restaurant receives multiple customer orders. |
| Customer | Historical Order | 1 : Many | A customer can place multiple orders. |
| Historical Order | Review | 1 : 0..1 | A completed order may receive one customer review or none. |
| Restaurant | Review | 1 : Many | A restaurant receives multiple customer reviews. |
| Customer | Review | 1 : Many | A customer can submit multiple reviews. |

---

# Entity Relationship (ER) Data Model

<p align="center">
  <img src="diagrams/3_ER_DataModel.png" alt="ER Data Model" width="950"/>
</p>

The Entity Relationship (ER) Data Model represents the logical and physical database design of the Restaurant Analytics platform. It defines the database tables, attributes, primary keys, foreign keys, constraints, and relationships required to implement the business model.

## Core Entities

- Restaurants
- Menu Items
- Historical Orders
- Customers
- Reviews

The ER model establishes:

- Primary Keys (PK)
- Foreign Keys (FK)
- Unique Constraints (UQ)
- Check Constraints (CK)
- One-to-Many Relationships
- Optional Relationships
- Referential Integrity

This data model serves as the foundation for the SQL database schema and the subsequent data engineering pipeline implemented throughout the project.

---

# Kimball Dimensional Modelling (Galaxy Schema)

<p align="center">
  <img src="diagrams/4_Dimensional_Modelling.png" alt="Kimball Dimensional Model" width="1000"/>
</p>

The Silver Layer of the Lakehouse follows the **Kimball Dimensional Modelling** methodology using a **Fact Constellation (Galaxy Schema)**. This model is optimized for analytical workloads and Power BI reporting by separating descriptive business attributes into dimension tables and transactional measurements into fact tables.

Unlike a traditional star schema, the galaxy schema contains multiple fact tables that share common conformed dimensions, allowing different business processes to be analyzed independently while maintaining consistent business definitions.

## Business Processes

The dimensional model supports three primary analytical business processes:

| Business Process | Fact Table | Grain |
|------------------|------------|-------|
| Order Processing | **fact_orders** | One row per customer order |
| Item-Level Sales | **fact_order_items** | One row per menu item within an order |
| Customer Feedback | **fact_reviews** | One row per customer review |

---

## Dimension Tables

Dimension tables provide descriptive business context that is shared across multiple fact tables.

| Dimension | Description |
|-----------|-------------|
| **dim_restaurants** | Restaurant location, branch information, contact details |
| **dim_customers** | Customer profile and demographic information |
| **dim_menu_items** | Menu item attributes including category, pricing, ingredients, and dietary information |

These are **conformed dimensions**, meaning they can be reused across multiple fact tables while maintaining consistent business definitions.

---

## Fact Tables

### fact_orders

Stores one record for every customer order.

Typical Measures:

- Total Order Amount
- Item Count
- Order Status
- Payment Method
- Order Type

**Grain**

> One row per customer order.

---

### fact_order_items

Captures detailed item-level sales within each order.

Typical Measures:

- Quantity
- Unit Price
- Subtotal

**Grain**

> One row for each menu item purchased within a customer order.

---

### fact_reviews

Stores customer feedback submitted after completed orders.

Typical Measures:

- Rating
- Sentiment Analysis
- Review Text
- Delivery Issue Indicator
- Food Quality Issue Indicator
- Pricing Issue Indicator

**Grain**

> One row per customer review associated with a completed order.

---

## Conformed Dimensions

The three fact tables share common dimensions, enabling integrated analytics across multiple business processes.

| Dimension | fact_orders | fact_order_items | fact_reviews |
|-----------|-------------|------------------|--------------|
| Restaurant | ✓ | ✓ | ✓ |
| Customer | ✓ | — | ✓ |
| Menu Item | — | ✓ | ✓ *(via item relationship)* |

This design enables analysts to drill down from high-level order metrics to individual menu item sales and customer feedback while maintaining consistent dimensional attributes across the entire analytics platform.

---

The Kimball dimensional model forms the **Silver Layer** of the Enterprise Restaurant Chain Analytics Lakehouse Platform and serves as the analytical foundation for downstream Gold Layer business metrics, dashboards, and executive reporting.
























# Databricks Database Ingestion Methods

I have used **Lakeflow Connect (CDC-enabled ingestion)** in my pipeline.

---

## 1. Lakeflow (CDC-enabled)
- Captures real-time changes (insert, update, delete) from source systems.
- Bronze stores raw CDC events as-is.
- Silver layer processes changes using CDF/streaming, including:
  - Data cleaning
  - Deduplication
  - SCD Type 1 / Type 2 modeling
- Best suited for modern near real-time lakehouse architectures.

---

## 2. Query-based (Watermark / Incremental)
- Used when CDC is not available in the source system.
- Uses incremental extraction based on `updated_at` (watermarking).
- Captures only new and updated records between runs.
- Deletes are handled via **soft deletes** (e.g., `is_deleted` flag).
- Best for batch-oriented incremental ingestion pipelines.

---

## 3. JDBC ingestion
- Direct ingestion from databases using JDBC connectors.
- Supports full load and incremental extraction (if supported by source).
- Common for legacy systems and simple ingestion needs.
- Less scalable and requires higher operational maintenance.

---

## 4. Federated querying (Lakehouse Federation)
- No data ingestion into Databricks.
- Data remains in the source system and is queried directly.
- Best for TB/PB-scale datasets or ad-hoc exploration.
- Performance depends on source system latency and capacity.

---

## Summary
- Lakeflow CDC      → Real-time ingestion (recommended modern approach)
- Watermarking      → Incremental batch ingestion
- JDBC              → Legacy ingestion approach
- Federation        → Query data without ingestion


# Lakeflow Connect - CDC Based

**Tables ingested via Lakeflow Connect into Bronze:** `historical_orders`, `reviews`
**Tables ingested via Lakeflow Connect into Silver:** `Customers`, `menu_items`, `restaurants`

---

## Step 1: Create a Source Connection

- Configure the connection to the source system.
- Example: Azure SQL Database, SQL Server, Salesforce, etc.
- Provide host, port, database, username, password, and authentication.

---

## Step 2: Create an Ingestion Pipeline

- Define pipeline name and Event Log location (stores pipeline execution logs and metrics).
- Choose the ingestion mode:

### CDC (Change Data Capture)

- Reads DML changes directly from the database transaction log.
- **Use when:** The source database supports CDC and you need near real-time synchronization.
- **Why:** Lowest source impact, captures all data changes (including deletes), and provides continuous ingestion.

### Query-based Capture

- Uses a cursor (watermark) column to query new or updated records on a schedule.
- **Use when:** CDC is unavailable, unsupported, or cannot be enabled on the source database.
- **Why:** Simple to configure, works with many databases, but relies on a watermark column and captures only the latest state of changed rows between scheduled runs.

---

## Step 3: Select Source Tables

For each source table, configure:

- **Cursor Column:** Watermark column used to detect newly inserted or updated records. Enables incremental ingestion.
- **Primary Key(s):** Unique identifier for each row. Used to identify records during incremental loads, merges, and updates.
- **History Tracking**
  - **OFF** → SCD Type 1 (keeps only the latest version of a record).
  - **ON** → SCD Type 2 (maintains historical versions of records, maintains all inserts, updates, and deletes).

---

## Step 4: Configure Destination

- Select the Unity Catalog catalog, schema, and destination Delta table.

---

## Step 5: Schedule & Notifications
- Configure pipeline schedule, alerts, and notifications.

