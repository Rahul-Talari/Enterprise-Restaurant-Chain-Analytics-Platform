"""
Unity Catalog governance for the restaurant analytics catalog.

Runs as two tasks of master_job:

  --stage setup   (before the pipelines)
      * governance and secure schemas
      * restaurant_access mapping table (which user may see which restaurant)
      * row-filter and column-masking functions
      * RBAC grants for the Owner, Analytics and Data Engineering groups

  --stage apply   (after the Gold pipeline, once all tables exist)
      * PII tags on customer columns
      * ABAC policy: mask every column tagged `pii` in the Gold schema for
        everyone except owners
      * dynamic views in the secure schema: restaurant-scoped, PII-masked
        views that analysts query instead of the Silver tables

Access model
  Owners            read everything, unmasked.
  Data Engineering  full access to Bronze/Silver/Gold; PII masked in Gold.
  Analytics         Gold d_restaurant_reviews (row-filtered) and the secure
                    views only; restaurant-scoped and PII masked.

Every statement is idempotent, so the task is safe to run every hour.
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--stage", choices=["setup", "apply"], required=True)
parser.add_argument("--catalog", required=True)
parser.add_argument("--bronze_schema", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--gold_schema", required=True)
parser.add_argument("--governance_schema", required=True)
parser.add_argument("--secure_schema", required=True)
parser.add_argument("--owners_group", required=True)
parser.add_argument("--analysts_group", required=True)
parser.add_argument("--engineers_group", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()


def q(name):
    """Backtick-quote one identifier part (schema names start with a digit)."""
    return f"`{name}`"


CAT = q(args.catalog)
BRONZE = f"{CAT}.{q(args.bronze_schema)}"
SILVER = f"{CAT}.{q(args.silver_schema)}"
GOLD = f"{CAT}.{q(args.gold_schema)}"
GOV = f"{CAT}.{q(args.governance_schema)}"
SECURE = f"{CAT}.{q(args.secure_schema)}"

OWNERS = args.owners_group
ANALYSTS = args.analysts_group
ENGINEERS = args.engineers_group


def run(sql):
    print(sql.strip().splitlines()[0][:150])
    spark.sql(sql)


# =============================================================================
# Stage: setup
# =============================================================================
def setup():

    # ---- Schemas --------------------------------------------------------------
    run(f"CREATE SCHEMA IF NOT EXISTS {GOV} COMMENT 'Access-control functions and mappings'")
    run(f"CREATE SCHEMA IF NOT EXISTS {SECURE} COMMENT 'Restaurant-scoped, PII-masked views for analysts'")

    # ---- Restaurant access mapping --------------------------------------------
    # One row per (user, restaurant). Owners maintain this table.
    run(f"""
        CREATE TABLE IF NOT EXISTS {GOV}.restaurant_access (
            principal     STRING NOT NULL COMMENT 'User email, as returned by current_user()',
            restaurant_id STRING NOT NULL COMMENT 'Restaurant the user may see'
        )
        COMMENT 'Restaurant-level access for analysts'
    """)

    # ---- Row filter ----------------------------------------------------------
    # Owners and data engineers see all restaurants; everyone else only the
    # restaurants mapped to them.
    run(f"""
        CREATE OR REPLACE FUNCTION {GOV}.restaurant_filter(rid STRING)
        RETURNS BOOLEAN
        COMMENT 'Row filter: restaurant-level isolation'
        RETURN is_account_group_member('{OWNERS}')
            OR is_account_group_member('{ENGINEERS}')
            OR EXISTS (
                SELECT 1 FROM {GOV}.restaurant_access a
                WHERE a.restaurant_id = rid
                  AND a.principal = current_user()
            )
    """)

    # ---- Column masks -----------------------------------------------------------
    run(f"""
        CREATE OR REPLACE FUNCTION {GOV}.mask_email(email STRING)
        RETURNS STRING
        COMMENT 'Column mask: owners see the email, others see j***@domain'
        RETURN CASE
            WHEN is_account_group_member('{OWNERS}') THEN email
            WHEN email IS NULL THEN NULL
            ELSE concat(left(email, 1), '***@', substring_index(email, '@', -1))
        END
    """)

    run(f"""
        CREATE OR REPLACE FUNCTION {GOV}.mask_phone(phone STRING)
        RETURNS STRING
        COMMENT 'Column mask: owners see the phone number, others see the last 4 digits'
        RETURN CASE
            WHEN is_account_group_member('{OWNERS}') THEN phone
            WHEN phone IS NULL THEN NULL
            ELSE concat('***-***-', right(regexp_replace(phone, '[^0-9]', ''), 4))
        END
    """)

    run(f"""
        CREATE OR REPLACE FUNCTION {GOV}.mask_pii(value STRING)
        RETURNS STRING
        COMMENT 'Generic PII mask used by the ABAC policy'
        RETURN CASE
            WHEN is_account_group_member('{OWNERS}') THEN value
            WHEN value IS NULL THEN NULL
            ELSE '***MASKED***'
        END
    """)

    # ---- RBAC ---------------------------------------------------------------------
    run(f"GRANT USE CATALOG ON CATALOG {CAT} TO `{OWNERS}`")
    run(f"GRANT USE CATALOG ON CATALOG {CAT} TO `{ENGINEERS}`")
    run(f"GRANT USE CATALOG ON CATALOG {CAT} TO `{ANALYSTS}`")

    # Data Engineering: build and operate every layer.
    for schema in (BRONZE, SILVER, GOLD, GOV, SECURE):
        run(f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO `{ENGINEERS}`")

    # Owners: read everything enterprise-wide; manage restaurant access.
    for schema in (BRONZE, SILVER, GOLD, SECURE):
        run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {schema} TO `{OWNERS}`")
    run(f"GRANT USE SCHEMA ON SCHEMA {GOV} TO `{OWNERS}`")
    run(f"GRANT SELECT, MODIFY ON TABLE {GOV}.restaurant_access TO `{OWNERS}`")

    # Analytics: no Bronze/Silver access. Secure views + row-filtered Gold only.
    run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {SECURE} TO `{ANALYSTS}`")
    run(f"GRANT USE SCHEMA ON SCHEMA {GOLD} TO `{ANALYSTS}`")


# =============================================================================
# Stage: apply
# =============================================================================
PII_COLUMNS = {
    # (schema, table): {column: tag value}
    (BRONZE, "customers"): {"name": "name", "email": "email", "phone": "phone"},
    (SILVER, "dim_customers"): {"name": "name", "email": "email", "phone": "phone"},
    (GOLD, "daily_customer_360"): {"customer_name": "name", "email": "email"},
}

# ALTER syntax differs for pipeline-managed objects.
ALTER_KEYWORD = {
    "STREAMING_TABLE": "STREAMING TABLE",
    "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
}


def table_type(schema_fqn, table):
    schema_name = schema_fqn.split(".")[1].strip("`")
    rows = spark.sql(f"""
        SELECT table_type FROM {CAT}.information_schema.tables
        WHERE table_schema = '{schema_name}' AND table_name = '{table}'
    """).collect()
    return rows[0][0] if rows else None


def apply():

    # ---- PII tags ---------------------------------------------------------------
    for (schema, table), columns in PII_COLUMNS.items():
        ttype = table_type(schema, table)
        if ttype is None:
            print(f"skip tags: {schema}.{table} does not exist yet")
            continue
        keyword = ALTER_KEYWORD.get(ttype, "TABLE")
        for column, value in columns.items():
            run(f"ALTER {keyword} {schema}.{table} ALTER COLUMN {column} SET TAGS ('pii' = '{value}')")

    # ---- ABAC: mask tagged PII in Gold for everyone except owners -----------------
    # Attribute-based: any current or future Gold column tagged `pii` is masked,
    # with no per-table setup. Requires `pii` to be a governed tag (see prerequisites.md).
    run(f"""
        CREATE OR REPLACE POLICY mask_pii_columns
        ON SCHEMA {GOLD}
        COMMENT 'Mask columns tagged pii for everyone except owners'
        COLUMN MASK {GOV}.mask_pii
        TO `account users`
        EXCEPT `{OWNERS}`
        FOR TABLES
        MATCH COLUMNS hasTag('pii') AS pii_col
        ON COLUMN pii_col
    """)

    # ---- Row-filtered Gold objects for analysts ------------------------------------
    run(f"GRANT SELECT ON TABLE {GOLD}.d_restaurant_reviews TO `{ANALYSTS}`")

    # Metric views (sources/7_semantic_layer) filter rows with restaurant_filter()
    for metric_view in ("mv_sales", "mv_menu", "mv_reviews"):
        run(f"GRANT SELECT ON TABLE {GOLD}.{metric_view} TO `{ANALYSTS}`")

    # ---- Dynamic views ------------------------------------------------------------
    # Each view filters rows with restaurant_filter() and masks PII, evaluated
    # for the user running the query.
    run(f"""
        CREATE OR REPLACE VIEW {SECURE}.v_orders
        COMMENT 'Orders for the restaurants the current user may see'
        AS SELECT * FROM {SILVER}.fact_orders
        WHERE {GOV}.restaurant_filter(restaurant_id)
    """)

    run(f"""
        CREATE OR REPLACE VIEW {SECURE}.v_order_items
        COMMENT 'Order items for the restaurants the current user may see'
        AS SELECT * FROM {SILVER}.fact_order_items
        WHERE {GOV}.restaurant_filter(restaurant_id)
    """)

    run(f"""
        CREATE OR REPLACE VIEW {SECURE}.v_reviews
        COMMENT 'Reviews and AI sentiment for the restaurants the current user may see'
        AS SELECT * FROM {SILVER}.fact_reviews
        WHERE {GOV}.restaurant_filter(restaurant_id)
    """)

    run(f"""
        CREATE OR REPLACE VIEW {SECURE}.v_daily_restaurant_sales
        COMMENT 'Daily sales per restaurant, restaurant-scoped'
        AS SELECT
            order_date,
            restaurant_id,
            count(DISTINCT order_id)         AS total_orders,
            sum(total_amount)                AS total_revenue,
            avg(total_amount)                AS avg_order_value,
            count(DISTINCT customer_id)      AS unique_customers
        FROM {SILVER}.fact_orders
        WHERE {GOV}.restaurant_filter(restaurant_id)
        GROUP BY order_date, restaurant_id
    """)

    run(f"""
        CREATE OR REPLACE VIEW {SECURE}.v_customers
        COMMENT 'Current customers who ordered at the user''s restaurants; email and phone masked'
        AS SELECT
            c.customer_id,
            c.name                        AS customer_name,
            {GOV}.mask_email(c.email)     AS email,
            {GOV}.mask_phone(c.phone)     AS phone,
            c.city,
            c.join_date
        FROM {SILVER}.dim_customers c
        WHERE c.`__END_AT` IS NULL
          AND EXISTS (
              SELECT 1 FROM {SILVER}.fact_orders o
              WHERE o.customer_id = c.customer_id
                AND {GOV}.restaurant_filter(o.restaurant_id)
          )
    """)


if args.stage == "setup":
    setup()
else:
    apply()

print(f"governance stage '{args.stage}' completed")
