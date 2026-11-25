from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")

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
# Data Quality Rules
# Every rule must hold for a row to reach fact_orders.
# Rows that break any rule go to fact_orders_quarantine,
# together with the names of the rules they broke.
# ---------------------------------------
ORDER_RULES = {
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


def with_failed_rules(df, rules):
    """Adds failed_rules (names of broken rules; NULL results count as broken) and keeps only failing rows."""
    checks = [when(~coalesce(expr(rule), lit(False)), lit(name)) for name, rule in rules.items()]
    return (
        df.withColumn("failed_rules", array_compact(array(*checks)))
          .filter(size(col("failed_rules")) > 0)
          .withColumn("_quarantined_at", current_timestamp())
    )

# ---------------------------------------
# Standardize Event Hub Orders
# ---------------------------------------
@dp.view
def orders_stream():

    return (
        spark.readStream.table(f"{CATALOG}.`{BRONZE_SCHEMA}`.orders")
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
        spark.readStream.table(f"{CATALOG}.`{BRONZE_SCHEMA}`.historical_orders")
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
# Unified Change Stream (before validation)
# ---------------------------------------
@dp.view
def unified_orders_raw():

    return (
        dp.read_stream("historical_orders_stream")
            .unionByName(
                dp.read_stream("orders_stream"),
                allowMissingColumns=True
            )
    )

# ---------------------------------------
# Validated Stream -> fact_orders
# ---------------------------------------
@dp.expect_all_or_drop(ORDER_RULES)
@dp.view
def unified_orders():
    return dp.read_stream("unified_orders_raw")

# ---------------------------------------
# Rejected Rows -> fact_orders_quarantine
# ---------------------------------------
@dp.table(
    name="fact_orders_quarantine",
    comment="Orders that failed data-quality rules, with the names of the failed rules",
    table_properties={"quality": "quarantine"}
)
def fact_orders_quarantine():
    return with_failed_rules(dp.read_stream("unified_orders_raw"), ORDER_RULES)

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