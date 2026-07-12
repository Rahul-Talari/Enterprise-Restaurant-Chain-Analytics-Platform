# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Table Schemas (Databricks)
# MAGIC
# MAGIC ## Gold Layer Table: `03_gold.d_sales_summary`
# MAGIC
# MAGIC ### Schema
# MAGIC
# MAGIC | Column             | Data Type           | Description                |
# MAGIC |--------------------|---------------------|----------------------------|
# MAGIC | order_date         | DATE (Primary Key)  | Date of the order          |
# MAGIC | total_orders       | BIGINT              | Total number of orders     |
# MAGIC | total_revenue      | DECIMAL(12,2)       | Total revenue              |
# MAGIC | avg_order_value    | DECIMAL(10,2)       | Average order value        |
# MAGIC | unique_customers   | BIGINT              | Unique customers           |
# MAGIC | unique_restaurants | BIGINT              | Unique restaurants         |
# MAGIC | dine_in_orders     | BIGINT              | Dine-in orders             |
# MAGIC | takeaway_orders    | BIGINT              | Takeaway orders            |
# MAGIC | delivery_orders    | BIGINT              | Delivery orders            |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **SQL DDL:**
# MAGIC ```
# MAGIC sql
# MAGIC CREATE TABLE `03_gold`.d_sales_summary (
# MAGIC     order_date DATE PRIMARY KEY,
# MAGIC     total_orders BIGINT,
# MAGIC     total_revenue DECIMAL(12,2),
# MAGIC     avg_order_value DECIMAL(10,2),
# MAGIC     unique_customers BIGINT,
# MAGIC     unique_restaurants BIGINT,
# MAGIC     dine_in_orders BIGINT,
# MAGIC     takeaway_orders BIGINT,
# MAGIC     delivery_orders BIGINT
# MAGIC )
# MAGIC ```

# COMMAND ----------

df_orders=spark.read.table("databricksproject_ws.`02_silver`.fact_orders")
display(df_orders.limit(5))

# COMMAND ----------

'''
order_date DATE PRIMARY KEY,
total_orders BIGINT,
total_revenue DECIMAL(12,2),
avg_order_value DECIMAL(10,2),
unique_customers BIGINT,
unique_restaurants BIGINT,
dine_in_orders BIGINT,
takeaway_orders BIGINT,
delivery_orders BIGINT
 '''
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

df_sales = (
    df_orders
        .groupBy("order_date")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_amount").cast(DecimalType(12, 2)).alias("total_revenue"),
            avg("total_amount").cast(DecimalType(10, 2)).alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("restaurant_id").alias("unique_restaurants"),
            sum(when(col("order_type") == "dine_in", 1).otherwise(0)).alias("dine_in_orders"),
            sum(when(col("order_type") == "takeaway", 1).otherwise(0)).alias("takeaway_orders"),
            sum(when(col("order_type") == "delivery", 1).otherwise(0)).alias("delivery_orders")
        )
)

df_sales = df_sales.select(
    "order_date",
    "total_orders",
    "total_revenue",
    "avg_order_value",
    "unique_customers",
    "unique_restaurants",
    "dine_in_orders",
    "takeaway_orders",
    "delivery_orders"
)

display(df_sales)

# COMMAND ----------

from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime


@dp.materialized_view(
    name="03_gold.d_sales_summary",
    partition_cols=["order_date"],
    table_properties={"quality": "gold"},
    comment="Gold layer aggregates with date-based overwrites",
)
def d_sales_summary():
    df_daily_agg = (
        dp.read("02_silver.fact_orders")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_amount").cast("decimal(10,2)").alias("total_revenue"),
            F.avg("total_amount").cast("decimal(10,2)").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("restaurant_id").alias("unique_restaurants"),
            F.sum(F.when(F.col("order_type") == "dine_in", 1).otherwise(0)).alias(
                "dine_in_orders"
            ),
            F.sum(F.when(F.col("order_type") == "takeaway", 1).otherwise(0)).alias(
                "takeaway_orders"
            ),
            F.sum(F.when(F.col("order_type") == "delivery", 1).otherwise(0)).alias(
                "delivery_orders"
            ),
        )
        .select(
            "order_date",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "unique_customers",
            "unique_restaurants",
            "dine_in_orders",
            "takeaway_orders",
            "delivery_orders",
        )
    )
    return df_daily_agg

