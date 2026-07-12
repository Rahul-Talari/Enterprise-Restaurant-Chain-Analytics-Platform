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
# Event Hub Orders (Retain items)
# ---------------------------------------
@dp.view
def orders_items_stream():

    return (
        spark.readStream.table("databricksproject_ws.`01_bronze`.orders")
            .withColumn("_ingestion_timestamp", current_timestamp())
    )

# ---------------------------------------
# Historical Orders (Parse items)
# ---------------------------------------
@dp.view
def historical_orders_items_stream():

    return (
        spark.readStream.table("databricksproject_ws.`01_bronze`.historical_orders")
            .withColumn("items", from_json(col("items"), ITEMS_SCHEMA))
            .withColumn("_ingestion_timestamp", current_timestamp())
    )

# ---------------------------------------
# Unified Order Items Stream
# ---------------------------------------
@dp.expect_all_or_drop(
    {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_item_id": "item_id IS NOT NULL",
        "valid_last_updated": "last_updated IS NOT NULL",
        "valid_order_timestamp": "order_timestamp IS NOT NULL",
        "valid_restaurant_id": "restaurant_id IS NOT NULL",
        "valid_quantity": "quantity > 0",
        "valid_unit_price": "unit_price > 0",
        "valid_subtotal": "subtotal > 0"
    }
)
@dp.view
def unified_order_items():

    return (
        dp.read_stream("historical_orders_items_stream")
            .unionByName(
                dp.read_stream("orders_items_stream"),
                allowMissingColumns=True
            )
            .withColumn("item", explode(col("items")))
            .select(
                "order_id",
                col("item.item_id").alias("item_id"),
                "restaurant_id",
                "order_timestamp",
                to_date("order_timestamp").alias("order_date"),
                col("item.quantity").alias("quantity"),
                col("item.unit_price").cast(DecimalType(10, 2)).alias("unit_price"),
                col("item.subtotal").cast(DecimalType(10, 2)).alias("subtotal"),
                "last_updated",
                "_ingestion_timestamp"
            )
    )
    
# ---------------------------------------
# Target Silver Table
# ---------------------------------------
dp.create_streaming_table(
    name="fact_order_items"
)

# ---------------------------------------
# AUTO CDC
# ---------------------------------------
dp.create_auto_cdc_flow(
    target="fact_order_items",
    source="unified_order_items",
    keys=["order_id", "item_id"],
    sequence_by=col("last_updated")
)