=========================================================================================

**ENTERPRISE RESTAURANT CHAIN ANALYTICS LAKEHOUSE PLATFORM**

=========================================================================================

**OLTP Data Modelling**

&#x09;    Conceptual Data Model  : Business Requirements

&#x09;    Logical Data Model     : ER Model (DBMS Independent)

&#x09;    Physical Data Model    : Azure SQL Database (3NF)



**OLAP Data Modelling**

&#x09;    Dimensional Model      : Kimball - Fact Constellation (Galaxy Schema)



**SOURCE SYSTEMS**



&#x09;Azure SQL Database

&#x09;    ├── restaurants

&#x09;    ├── menu\_items

&#x09;    ├── customers

&#x09;    ├── historical\_orders

&#x09;    └── reviews



&#x09;Azure Event Hubs

&#x09;    └── orders (Real-Time Order Stream)

**===========================================================================================================================**



**BRONZE LAYER :  Objects - Streaming Tables; Compute - Serverless**



&#x09;Purpose  : Ingest raw source data with minimal transformation.

&#x09;Ingestion:

&#x09;	Azure SQL Database : Initial Full Load followed by Incremental CDC ingestion using Lakeflow Connect.

&#x09;	Azure Event Hubs   : Real-time streaming ingestion using Lakeflow Spark Declarative Pipelines (SDP).

&#x09;Bronze Tables

&#x09;    ├── restaurants

&#x09;    ├── menu\_items

&#x09;    ├── customers

&#x09;    ├── historical\_orders

&#x09;    ├── reviews

&#x09;    └── orders

**===========================================================================================================================**





**SILVER LAYER : Objects - Streaming Tables; Compute - Serverless; Data Quality; Dimensional Modelling, SCD-2**



&#x09;Purpose : Cleanse, standardize, enrich, and model data for analytics.



&#x09;**Fact Tables**

&#x09;	fact\_orders      = Combines bronze.orders + bronze.historical\_orders + SCD Type 2 + Lakeflow Spark Declarative Pipelines (SDP) + Data Quality Checks

&#x09;	fact\_order\_items = combine bronze.orders + bronze.historical\_orders + Parse Order Item JSON + SCD Type 2 + Lakeflow Spark Declarative Pipelines (SDP) + Data Quality Checks

&#x09;	fact\_reviews     = Source: bronze.reviews + SCD Type 2 + Lakeflow Spark Declarative Pipelines (SDP) + Data Quality Checks + Mosaic AI Sentiment Analysis



&#x09;**Dimension Tables**

&#x09;	dim\_restaurants = Source: bronze.restaurants + SCD Type 2 + Lakeflow Spark Declarative Pipelines (SDP) + Data Quality Checks

&#x09;	dim\_customers   = Source: bronze.customers + SCD Type 2 + Lakeflow Spark Declarative Pipelines (SDP) + Data Quality Checks

&#x09;	dim\_menu\_items  = Source: bronze.menu\_items + SCD Type 2 + Lakeflow Spark Declarative Pipelines (SDP) + Data Quality Checks

**===========================================================================================================================**





**GOLD LAYER : Objects - Materialized Views; Compute - Serverless; Optimizations - Liquid Clustering on Tables; Vaccum; AQE (for joins, skew handling, Partition Tuning)**



&#x09;	Purpose:    Business-ready analytical datasets optimized for reporting and AI/BI.

&#x09;	gold\_daily\_sales\_summary       = Source: fact\_orders

&#x09;	gold\_daily\_restaurant\_reviews  = Source: fact\_reviews + dim\_restaurants

&#x09;	gold\_daily\_customer\_360 	  = Source: fact\_orders + fact\_order\_items + fact\_reviews + dim\_customers +  dim\_restaurants

**===========================================================================================================================**



**ORCHESTRATION \& DEVOPS**

&#x09;	Lakeflow Jobs : Orchestrates Bronze, Silver, and Gold pipelines; Daily Scheduled Refreshes; Failure Alerts \& Notifications; Job Monitoring

&#x09;	Unity Catalog : Centralized Governance; RBAC; PII Masking; Delta Sharing; Data Lineage

&#x09;	Storage       : ADLS Gen2 with Delta Lake Tables governed by Unity Catalog External Locations and secured using Azure Databricks Access Connector.

&#x09;	Databricks Asset Bundles (DABs)

&#x09;		    ├── Infrastructure as Code (IaC)

&#x09;		    ├── CI/CD Pipeline Automation

&#x09;		    ├── Environment-specific Deployments (Dev / Test / Prod)

&#x09;		    └── GitHub Integration

**===========================================================================================================================**





**CONSUMPTION LAYER : Cluster - Serverless SQL Warehouse**

Consumption

&#x20;   ├── Power BI Dashboards

&#x20;   ├── AI/BI Genie

&#x20;   └── Business Users



Business users consume curated Gold Layer data through AI/BI Dashboards \& AI-powered natural language analytics.

**===========================================================================================================================**

