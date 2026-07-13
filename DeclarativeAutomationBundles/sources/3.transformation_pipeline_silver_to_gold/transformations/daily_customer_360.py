from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(
    name="daily_customer_360",
    comment="Customer 360 analytics for reporting and AI/BI dashboards"
)
def daily_customer_360():

    # -------------------------
    # Read Silver Tables
    # -------------------------

    customers_src = spark.read.table("databricksproject_ws.`02_silver`.dim_customers")
    restaurants_src = spark.read.table("databricksproject_ws.`02_silver`.dim_restaurants")
    menu_items_src = spark.read.table("databricksproject_ws.`02_silver`.dim_menu_items")
    orders_src = spark.read.table("databricksproject_ws.`02_silver`.fact_orders")
    order_items_src = spark.read.table("databricksproject_ws.`02_silver`.fact_order_items")
    reviews_src = spark.read.table("databricksproject_ws.`02_silver`.fact_reviews")

    # -------------------------
    # Current Dimension Records
    # -------------------------

    current_customers = (
        customers_src
        .filter(col("__END_AT").isNull())
        .select(
            "customer_id",
            col("name").alias("customer_name"),
            "email",
            "city",
            "join_date"
        )
    )

    current_restaurants = (
        restaurants_src
        .filter(col("__END_AT").isNull())
        .select(
            "restaurant_id",
            col("name").alias("restaurant_name")
        )
    )

    current_menu_items = (
        menu_items_src
        .filter(col("__END_AT").isNull())
        .select(
            "item_id",
            col("name").alias("item_name")
        )
    )

    # -------------------------
    # Customer Sales Metrics
    # -------------------------

    df_order_sales = (
        orders_src
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            round(sum("total_amount"), 2).alias("lifetime_spend"),
            round(avg("total_amount"), 2).alias("avg_order_value"),
            max("order_date").alias("last_order_date")
        )
        .withColumn(
            "is_at_risk",
            datediff(current_date(), col("last_order_date")) >= 90
        )
        .withColumn(
            "loyalty_tier",
            when(col("lifetime_spend") >= 50000, "VIP")
            .otherwise("Standard")
        )
    )

    # -------------------------
    # Favorite Restaurant
    # -------------------------

    df_fav_restaurant = (
        orders_src
        .groupBy("customer_id", "restaurant_id")
        .agg(countDistinct("order_id").alias("order_ct"))
        .join(current_restaurants, "restaurant_id", "left")
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("customer_id")
                .orderBy(desc("order_ct"), asc("restaurant_name"))
            )
        )
        .filter(col("rn") == 1)
        .select(
            "customer_id",
            col("restaurant_name").alias("favorite_restaurant")
        )
    )

    # -------------------------
    # Review Metrics
    # -------------------------

    df_avg_rating = (
        reviews_src
        .groupBy("customer_id")
        .agg(
            round(avg("rating"), 2).alias("avg_rating_given"),
            countDistinct("review_id").alias("total_reviews")
        )
    )

    # -------------------------
    # Favorite Menu Item
    # -------------------------

    df_fav_item = (
        order_items_src
        .join(
            orders_src.select("order_id", "customer_id"),
            "order_id"
        )
        .groupBy("customer_id", "item_id")
        .agg(sum("quantity").alias("item_qty"))
        .join(current_menu_items, "item_id", "left")
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("customer_id")
                .orderBy(desc("item_qty"), asc("item_name"))
            )
        )
        .filter(col("rn") == 1)
        .select(
            "customer_id",
            col("item_name").alias("favorite_item")
        )
    )

    # -------------------------
    # Customer 360
    # -------------------------

    return (
        current_customers
        .join(df_order_sales, "customer_id", "left")
        .join(df_fav_restaurant, "customer_id", "left")
        .join(df_avg_rating, "customer_id", "left")
        .join(df_fav_item, "customer_id", "left")
        .select(
            "customer_id",
            "customer_name",
            "email",
            "city",
            "loyalty_tier",
            "join_date",
            coalesce(col("total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("lifetime_spend"), lit(0.00)).alias("lifetime_spend"),
            coalesce(col("avg_order_value"), lit(0.00)).alias("avg_order_value"),
            "last_order_date",
            "favorite_restaurant",
            "favorite_item",
            coalesce(col("avg_rating_given"), lit(0.00)).alias("avg_rating_given"),
            coalesce(col("total_reviews"), lit(0)).alias("total_reviews"),
            coalesce(col("is_at_risk"), lit(False)).alias("is_at_risk")
        )
    )