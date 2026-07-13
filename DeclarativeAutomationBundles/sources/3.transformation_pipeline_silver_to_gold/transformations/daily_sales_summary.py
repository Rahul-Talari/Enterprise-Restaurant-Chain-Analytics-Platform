from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

@dp.materialized_view(
    name="03_gold.d_sales_summary",
    partition_cols=["order_date"],
    table_properties={"quality": "gold"},
    comment="Gold layer aggregates with date-based overwrites"
)
def d_sales_summary():

    return (
        spark.read.table("02_silver.fact_orders")
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
            .select(
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
    )