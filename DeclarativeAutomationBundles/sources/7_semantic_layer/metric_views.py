"""
Semantic layer: Unity Catalog metric views in the Gold schema.

Each metric view defines its business dimensions and measures once, in YAML.
AI/BI dashboards, Genie and SQL users all query the same definitions, so a
KPI such as "average order value" is calculated the same way everywhere:

    SELECT restaurant_name,
           MEASURE(total_revenue)     AS revenue,
           MEASURE(avg_order_value)   AS aov
    FROM <catalog>.`03_gold`.mv_sales
    GROUP BY restaurant_name;

Restaurant-level isolation: every view filters rows with the same
restaurant_filter() function used by the secure views, so analysts see only
their restaurants while owners and engineers see the whole chain.

Runs as the `metric_views` task of master_job, after the Gold pipeline.
Grants are applied by governance.py (stage "apply").
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--gold_schema", required=True)
parser.add_argument("--governance_schema", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

SILVER = f"`{args.catalog}`.`{args.silver_schema}`"
GOLD = f"`{args.catalog}`.`{args.gold_schema}`"
ROW_FILTER = f"`{args.catalog}`.`{args.governance_schema}`.restaurant_filter(restaurant_id)"

# Current version of each SCD Type 2 dimension
CURRENT = "__END_AT IS NULL"

METRIC_VIEWS = {

    # -------------------------------------------------------------------------
    # Sales KPIs per order
    # -------------------------------------------------------------------------
    "mv_sales": f"""
version: 0.1
source: "{SILVER}.fact_orders"
filter: "{ROW_FILTER}"

joins:
  - name: restaurant
    source: "{SILVER}.dim_restaurants"
    on: source.restaurant_id = restaurant.restaurant_id AND restaurant.{CURRENT}

dimensions:
  - name: order_date
    expr: order_date
  - name: order_month
    expr: DATE_TRUNC('MONTH', order_date)
  - name: day_of_week
    expr: day_of_week
  - name: order_hour
    expr: order_hour
  - name: is_weekend
    expr: is_weekend
  - name: order_type
    expr: order_type
  - name: payment_method
    expr: payment_method
  - name: order_status
    expr: order_status
  - name: restaurant_id
    expr: restaurant_id
  - name: restaurant_name
    expr: restaurant.name
  - name: restaurant_city
    expr: restaurant.city
  - name: restaurant_country
    expr: restaurant.country

measures:
  - name: total_orders
    expr: COUNT(DISTINCT order_id)
  - name: total_revenue
    expr: SUM(total_amount)
  - name: avg_order_value
    expr: SUM(total_amount) / COUNT(DISTINCT order_id)
  - name: unique_customers
    expr: COUNT(DISTINCT customer_id)
  - name: items_sold
    expr: SUM(items_count)
  - name: avg_items_per_order
    expr: SUM(items_count) / COUNT(DISTINCT order_id)
  - name: weekend_revenue_share
    expr: SUM(CASE WHEN is_weekend THEN total_amount ELSE 0 END) / SUM(total_amount)
""",

    # -------------------------------------------------------------------------
    # Menu performance per order item
    # -------------------------------------------------------------------------
    "mv_menu": f"""
version: 0.1
source: "{SILVER}.fact_order_items"
filter: "{ROW_FILTER}"

joins:
  - name: menu_item
    source: "{SILVER}.dim_menu_items"
    on: source.restaurant_id = menu_item.restaurant_id AND source.item_id = menu_item.item_id AND menu_item.{CURRENT}
  - name: restaurant
    source: "{SILVER}.dim_restaurants"
    on: source.restaurant_id = restaurant.restaurant_id AND restaurant.{CURRENT}

dimensions:
  - name: order_date
    expr: order_date
  - name: order_month
    expr: DATE_TRUNC('MONTH', order_date)
  - name: item_name
    expr: menu_item.name
  - name: category
    expr: menu_item.category
  - name: is_vegetarian
    expr: menu_item.is_vegetarian
  - name: spice_level
    expr: menu_item.spice_level
  - name: restaurant_id
    expr: restaurant_id
  - name: restaurant_name
    expr: restaurant.name

measures:
  - name: quantity_sold
    expr: SUM(quantity)
  - name: item_revenue
    expr: SUM(subtotal)
  - name: orders_with_item
    expr: COUNT(DISTINCT order_id)
  - name: avg_unit_price
    expr: SUM(subtotal) / SUM(quantity)
""",

    # -------------------------------------------------------------------------
    # Review and sentiment KPIs per review
    # -------------------------------------------------------------------------
    "mv_reviews": f"""
version: 0.1
source: "{SILVER}.fact_reviews"
filter: "{ROW_FILTER}"

joins:
  - name: restaurant
    source: "{SILVER}.dim_restaurants"
    on: source.restaurant_id = restaurant.restaurant_id AND restaurant.{CURRENT}

dimensions:
  - name: review_date
    expr: DATE(review_timestamp)
  - name: review_month
    expr: DATE_TRUNC('MONTH', review_timestamp)
  - name: sentiment
    expr: sentiment
  - name: rating
    expr: rating
  - name: restaurant_id
    expr: restaurant_id
  - name: restaurant_name
    expr: restaurant.name

measures:
  - name: total_reviews
    expr: COUNT(DISTINCT review_id)
  - name: avg_rating
    expr: AVG(rating)
  - name: positive_share
    expr: COUNT_IF(sentiment = 'positive') / COUNT(DISTINCT review_id)
  - name: negative_share
    expr: COUNT_IF(sentiment = 'negative') / COUNT(DISTINCT review_id)
  - name: delivery_issues
    expr: COUNT_IF(issue_delivery)
  - name: food_quality_issues
    expr: COUNT_IF(issue_food_quality)
  - name: pricing_issues
    expr: COUNT_IF(issue_pricing)
  - name: portion_size_issues
    expr: COUNT_IF(issue_portion_size)
""",
}

for name, yaml_spec in METRIC_VIEWS.items():
    print(f"creating metric view {GOLD}.{name}")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {GOLD}.{name}
        WITH METRICS
        LANGUAGE YAML
        AS $$
{yaml_spec}
$$
    """)

print(f"created {len(METRIC_VIEWS)} metric views")
