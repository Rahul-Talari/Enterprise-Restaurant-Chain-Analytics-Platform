"""
Data-quality alert for master_job.

Counts the rows quarantined by the Silver pipeline during the last run window,
grouped by table and failed rule. If the total exceeds the threshold, the task
fails, and master_job's on-failure email sends the summary.

The task runs alongside silver_to_gold, so a DQ alert never blocks the Gold refresh.
"""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--window_minutes", type=int, default=65)  # hourly job + margin
parser.add_argument("--threshold", type=int, default=0)        # alert when more rows than this fail
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

QUARANTINE_TABLES = ["fact_orders_quarantine", "fact_order_items_quarantine"]

summary = []
total = 0

for table in QUARANTINE_TABLES:
    fqn = f"`{args.catalog}`.`{args.silver_schema}`.{table}"
    if not spark.catalog.tableExists(fqn):
        print(f"skip: {fqn} does not exist yet")
        continue

    rows = spark.sql(f"""
        SELECT rule, count(*) AS failed_rows
        FROM (
            SELECT explode(failed_rules) AS rule
            FROM {fqn}
            WHERE _quarantined_at >= current_timestamp() - INTERVAL {args.window_minutes} MINUTES
        )
        GROUP BY rule
        ORDER BY failed_rows DESC
    """).collect()

    table_rows = spark.sql(f"""
        SELECT count(*) FROM {fqn}
        WHERE _quarantined_at >= current_timestamp() - INTERVAL {args.window_minutes} MINUTES
    """).collect()[0][0]

    total += table_rows
    for r in rows:
        summary.append(f"  {table:<30} {r['rule']:<25} {r['failed_rows']} rows")

print(f"Quarantined rows in the last {args.window_minutes} minutes: {total}")
print("\n".join(summary) or "  none")

if total > args.threshold:
    raise RuntimeError(
        f"DQ alert: {total} rows quarantined in the last {args.window_minutes} minutes "
        f"(threshold {args.threshold}).\n" + "\n".join(summary)
    )
