# Databricks notebook source
restaurants=spark.read.table("databricksproject_ws.`01_bronze`.restaurants")
customers=spark.read.table("databricksproject_ws.`01_bronze`.customers")
menu_items=spark.read.table("databricksproject_ws.`01_bronze`.menu_items")

# COMMAND ----------

display(restaurants.limit(1))
display(customers.limit(1))
display(menu_items.limit(1))

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col

# =====================================================
# Bronze Streaming Views
# =====================================================

@dp.view
def restaurants_stream():
    return spark.readStream.table("databricksproject_ws.`01_bronze`.restaurants")


@dp.view
def customers_stream():
    return spark.readStream.table("databricksproject_ws.`01_bronze`.customers")


@dp.view
def menu_items_stream():
    return spark.readStream.table("databricksproject_ws.`01_bronze`.menu_items")


# =====================================================
# Restaurant Dimension (SCD Type 2)
# =====================================================

dp.create_streaming_table(
    name="dim_restaurants",
    comment="Restaurant Dimension (SCD Type 2)"
)

dp.create_auto_cdc_flow(
    target="dim_restaurants",
    source="restaurants_stream",
    keys=["restaurant_id"],
    sequence_by=col("last_updated"),
    stored_as_scd_type=2
)


# =====================================================
# Customer Dimension (SCD Type 2)
# =====================================================

dp.create_streaming_table(
    name="dim_customers",
    comment="Customer Dimension (SCD Type 2)"
)

dp.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_stream",
    keys=["customer_id"],
    sequence_by=col("last_updated"),
    stored_as_scd_type=2
)


# =====================================================
# Menu Item Dimension (SCD Type 2)
# =====================================================

dp.create_streaming_table(
    name="dim_menu_items",
    comment="Menu Item Dimension (SCD Type 2)"
)

dp.create_auto_cdc_flow(
    target="dim_menu_items",
    source="menu_items_stream",
    keys=["item_id"],
    sequence_by=col("last_updated"),
    stored_as_scd_type=2
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksproject_ws.`02_silver`.dim_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksproject_ws.`02_silver`.dim_menu_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksproject_ws.`02_silver`.dim_restaurants;
