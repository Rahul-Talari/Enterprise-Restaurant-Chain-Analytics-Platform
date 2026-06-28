from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(name="fact_orders", table_properties={"quality":"silver"})
@dp.expect_all_or_drop(
    {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_order_timestamp": "order_timestamp IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_restaurant_id": "restaurant_id IS NOT NULL",
        "valid_item_count": "item_count > 0",
        "valid_total_amount": "total_amount > 0"
    }
)
def fact_orders():

    df = spark.read.table("ws_restaurant_dbxproject.`01_bronze`.orders")
    
    df_fact_orders = df.withColumns({
        "order_timestamp": to_timestamp(col("order_timestamp")),
        "order_date": to_date(col("order_timestamp")),
        "order_hour": hour(col("order_timestamp")),
        "day_of_week": date_format(col("order_timestamp"), "EEEE"),
        "is_weekend": when(col("day_of_week").isin(["Saturday", "Sunday"]), True).otherwise(False),
        "items_parsed": col("items"),
        "item_count": size(col("items"))
    }).select(
        "order_id", "order_timestamp", "order_date", "order_hour", "day_of_week", "is_weekend",
        "restaurant_id", "customer_id", "order_type", "items_parsed", "item_count",
        "total_amount", "payment_method", "order_status"
    )

    return df_fact_orders
