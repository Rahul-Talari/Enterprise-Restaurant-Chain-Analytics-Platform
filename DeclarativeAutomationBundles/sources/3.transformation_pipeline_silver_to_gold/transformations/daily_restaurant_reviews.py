from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")
GOVERNANCE_SCHEMA = spark.conf.get("governance_schema")

# Restaurant-level isolation: users see only the restaurants mapped to them in
# <governance_schema>.restaurant_access (owners and data engineers see all).
# The function is created by sources/5_governance/governance.py (stage "setup").
ROW_FILTER = f"ROW FILTER {CATALOG}.`{GOVERNANCE_SCHEMA}`.restaurant_filter ON (restaurant_id)"


@dp.materialized_view(
    name="d_restaurant_reviews",
    row_filter=ROW_FILTER,
    table_properties={"quality": "gold"}
)
def d_restaurant_reviews():

    # Current Restaurant Dimension (SCD Type 2)
    df_restaurants = (
        spark.read.table(f"{CATALOG}.`{SILVER_SCHEMA}`.dim_restaurants")
             .filter(col("__END_AT").isNull())
    )

    # Review Fact
    df_reviews = spark.read.table(f"{CATALOG}.`{SILVER_SCHEMA}`.fact_reviews")

    # Aggregate Review Statistics
    df_review_stats = (
        df_reviews
            .groupBy("restaurant_id")
            .agg(
                count("review_id").alias("total_reviews"),
                round(avg("rating"), 2).cast(DecimalType(3,2)).alias("avg_rating"),

                sum(when(col("rating") == 5, 1).otherwise(0)).alias("rating_5_count"),
                sum(when(col("rating") == 4, 1).otherwise(0)).alias("rating_4_count"),
                sum(when(col("rating") == 3, 1).otherwise(0)).alias("rating_3_count"),
                sum(when(col("rating") == 2, 1).otherwise(0)).alias("rating_2_count"),
                sum(when(col("rating") == 1, 1).otherwise(0)).alias("rating_1_count"),

                sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_count"),
                sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_count"),
                sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_count")
            )
    )

    # Build Gold Dimension
    df_restaurant_reviews = (
        df_restaurants
            .join(df_review_stats, "restaurant_id", "left")
            .select(
                col("restaurant_id"),
                col("name").alias("restaurant_name"),
                col("city"),

                coalesce(col("total_reviews"), lit(0)).cast("bigint").alias("total_reviews"),
                coalesce(col("avg_rating"), lit(0)).cast(DecimalType(3,2)).alias("avg_rating"),

                coalesce(col("rating_5_count"), lit(0)).cast("bigint").alias("rating_5_count"),
                coalesce(col("rating_4_count"), lit(0)).cast("bigint").alias("rating_4_count"),
                coalesce(col("rating_3_count"), lit(0)).cast("bigint").alias("rating_3_count"),
                coalesce(col("rating_2_count"), lit(0)).cast("bigint").alias("rating_2_count"),
                coalesce(col("rating_1_count"), lit(0)).cast("bigint").alias("rating_1_count"),

                coalesce(col("sentiment_positive_count"), lit(0)).cast("bigint").alias("sentiment_positive_count"),
                coalesce(col("sentiment_neutral_count"), lit(0)).cast("bigint").alias("sentiment_neutral_count"),
                coalesce(col("sentiment_negative_count"), lit(0)).cast("bigint").alias("sentiment_negative_count")
            )
    )

    return df_restaurant_reviews