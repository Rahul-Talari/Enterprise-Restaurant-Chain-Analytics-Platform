from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

ITEMS_SCHEMA = """
ARRAY<
    STRUCT<
        item_id: STRING,
        name: STRING,
        category: STRING,
        quantity: INT,
        unit_price: DECIMAL(10,2),
        subtotal: DECIMAL(10,2)
    >
>
"""

# ---------------------------------------
# Standardize Event Hub Orders
# ---------------------------------------
@dp.view
def orders_stream():

    return (
        spark.readStream.table("databricksproject_ws.`01_bronze`.orders")
            .withColumn("items_count", size(col("items")))
            .withColumn("total_amount", col("total_amount").cast(DecimalType(10,2)))
            .withColumn("order_timestamp", to_timestamp("order_timestamp"))
            .withColumn("order_date", to_date("order_timestamp"))
            .withColumn("order_hour", hour("order_timestamp"))
            .withColumn("day_of_week", date_format("order_timestamp", "EEEE"))
            .withColumn(
                "is_weekend",
                when(dayofweek("order_timestamp").isin(1, 7), True).otherwise(False)
            )
            .withColumn("_ingestion_timestamp", current_timestamp())
            .drop("items")
    )

# ---------------------------------------
# Standardize Historical Orders
# ---------------------------------------
@dp.view
def historical_orders_stream():

    return (
        spark.readStream.table("databricksproject_ws.`01_bronze`.historical_orders")
            .withColumn("items", from_json(col("items"), ITEMS_SCHEMA))
            .withColumn("items_count", size(col("items")))
            .withColumn("total_amount", col("total_amount").cast(DecimalType(10,2)))
            .withColumn("order_timestamp", to_timestamp("order_timestamp"))
            .withColumn("order_date", to_date("order_timestamp"))
            .withColumn("order_hour", hour("order_timestamp"))
            .withColumn("day_of_week", date_format("order_timestamp", "EEEE"))
            .withColumn(
                "is_weekend",
                when(dayofweek("order_timestamp").isin(1, 7), True).otherwise(False)
            )
            .withColumn("_ingestion_timestamp", current_timestamp())
            .drop("items")
    )

# ---------------------------------------
# Unified Change Stream
# ---------------------------------------
@dp.expect_all_or_drop(
    {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_last_updated": "last_updated IS NOT NULL",
        "valid_order_timestamp": "order_timestamp IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_restaurant_id": "restaurant_id IS NOT NULL",
        "valid_item_count": "items_count > 0",
        "valid_order_status": "lower(order_status) IN ('completed','pending','ready','delivered','preparing','confirmed')",
        "valid_payment_method": "lower(payment_method) IN ('cash','card','wallet')",
        "valid_amount": "total_amount > 0"
    }
)
@dp.view
def unified_orders():

    return (
        dp.read_stream("historical_orders_stream")
            .unionByName(
                dp.read_stream("orders_stream"),
                allowMissingColumns=True
            )
    )

# ---------------------------------------
# Target Silver Table
# ---------------------------------------
dp.create_streaming_table(
    name="fact_orders"
)

# ---------------------------------------
# AUTO CDC
# ---------------------------------------
dp.create_auto_cdc_flow(
    target="fact_orders",
    source="unified_orders",
    keys=["order_id"],
    sequence_by=col("last_updated")
)