# Enterprise Restaurant Chain Analytics Lakehouse Platform

## Overview

This document provides a detailed walkthrough of the end-to-end architecture implemented for the **Enterprise Restaurant Chain Analytics Lakehouse Platform**.

The platform combines **batch and real-time data processing** using **Azure Databricks**, **Lakeflow Connect**, **Lakeflow Spark Declarative Pipelines (SDP)**, **Delta Lake**, **Unity Catalog**, and **Databricks Asset Bundles (DABs)** to build a scalable, governed, and production-style analytics platform.

---

# Data Modeling

## OLTP Data Modeling

| Model | Purpose |
|--------|---------|
| Conceptual Data Model | Capture business requirements and entities |
| Logical Data Model | Define relationships independent of the database platform |
| Physical Data Model | Azure SQL Database (3NF) |

---

## OLAP Data Modeling

| Model | Purpose |
|--------|---------|
| Kimball Fact Constellation (Galaxy Schema) | Analytical model for reporting and business intelligence |

---

# Architecture Pattern

```text
Medallion Architecture

Bronze
   │
   ▼
Silver
   │
   ▼
Gold
```

---

# Source Systems

## Azure SQL Database (Batch)

Operational tables

```text
restaurants
menu_items
customers
historical_orders
reviews
```

**Ingestion Method**

- Initial Full Load
- Incremental CDC
- Lakeflow Connect

---

## Azure Event Hubs (Streaming)

Streaming source

```text
orders
```

**Ingestion Method**

- Azure Event Hubs
- Kafka-compatible endpoint
- Lakeflow Spark Declarative Pipelines (SDP)

---

# Bronze Layer

**Objects**

- Streaming Tables

**Compute**

- Serverless

**Purpose**

Ingest raw source data while preserving source fidelity with minimal transformations.

## Data Ingestion

| Source | Method |
|---------|--------|
| Azure SQL Database | Initial Full Load followed by Incremental CDC using Lakeflow Connect |
| Azure Event Hubs | Real-time Streaming using Lakeflow Spark Declarative Pipelines (SDP) |

## Bronze Tables

```text
restaurants
menu_items
customers
historical_orders
reviews
orders
```

---

# Silver Layer

**Objects**

- Streaming Tables

**Compute**

- Serverless

**Purpose**

Cleanse, standardize, enrich, validate, and model data for downstream analytics.

## Features

- Data Quality Validation
- Schema Enforcement
- SCD Type 2
- Dimensional Modeling
- Streaming Transformations

## Fact Tables

| Table | Description |
|--------|-------------|
| fact_orders | Combines bronze.orders and bronze.historical_orders using SCD Type 2, SDP, and Data Quality Validation |
| fact_order_items | Parses nested order items and combines historical and streaming orders using SCD Type 2 |
| fact_reviews | Customer reviews enriched using Mosaic AI Sentiment Analysis, SCD Type 2, and Data Quality Validation |

## Dimension Tables

| Table | Description |
|--------|-------------|
| dim_restaurants | Restaurant master data with SCD Type 2 |
| dim_customers | Customer master data with SCD Type 2 |
| dim_menu_items | Menu catalog with SCD Type 2 |

---

# Gold Layer

**Objects**

- Materialized Views

**Compute**

- Serverless

**Purpose**

Business-ready analytical datasets optimized for reporting, self-service analytics, and AI/BI workloads.

## Optimizations

- Liquid Clustering
- Adaptive Query Execution (AQE)
- VACUUM
- OPTIMIZE

## Gold Data Products

| Dataset | Source |
|---------|--------|
| gold_daily_sales_summary | fact_orders |
| gold_daily_restaurant_reviews | fact_reviews + dim_restaurants |
| gold_daily_customer_360 | fact_orders + fact_order_items + fact_reviews + dim_customers + dim_restaurants |

---

# Orchestration & DevOps

## Lakeflow Jobs

Responsible for

- Pipeline Orchestration
- Scheduled Refreshes
- Streaming Pipeline Monitoring
- Failure Notifications
- Job Monitoring

---

## Unity Catalog

Provides centralized governance through

- Role-Based Access Control (RBAC)
- PII Masking
- Delta Sharing
- Data Lineage
- External Locations

---

## Storage

- Azure Data Lake Storage Gen2
- Delta Lake Tables
- Unity Catalog External Locations
- Azure Databricks Access Connector

---

## Databricks Asset Bundles (DABs)

Provides

- Infrastructure as Code (IaC)
- CI/CD Pipeline Automation
- Environment-specific Deployments (Dev/Test/Prod)
- GitHub Integration
- Configuration Management

---

# Consumption Layer

**Compute**

- Serverless SQL Warehouse

Business users consume curated Gold layer datasets through

- Power BI Dashboards
- Databricks AI/BI Genie
- Business Users

These datasets support KPI reporting, restaurant performance analysis, Customer 360 analytics, review insights, and AI-powered natural language querying.

---

# End-to-End Data Flow

```text
                 Azure SQL Database
                         │
        Lakeflow Connect (CDC)
                         │
                         ▼
                Bronze Streaming Tables
                         ▲
                         │
Azure Event Hubs ──► Spark Declarative Pipelines
                         │
                         ▼
                Bronze Streaming Tables
                         │
                         ▼
                Silver Streaming Tables
                         │
                         ▼
               Gold Materialized Views
                         │
         ┌───────────────┴───────────────┐
         ▼                               ▼
      AI/BI Dashboards                AI/BI Genie
```

---

# Summary

This project demonstrates a production-style Azure Databricks Lakehouse implementation that integrates **batch ingestion**, **real-time streaming**, **dimensional modeling**, **data governance**, **Infrastructure as Code**, and **AI-powered analytics** into a scalable enterprise data platform.