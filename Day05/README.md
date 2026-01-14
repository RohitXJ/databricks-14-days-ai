# Day 05 – Delta Lake Advanced

## Overview

This notebook explores advanced Delta Lake features. It demonstrates how to perform incremental `MERGE` operations, query historical data using Time Travel, optimize table performance with `OPTIMIZE`, and clean up old data files with `VACUUM`.

![Image:](5.png)

## Operations

### Step 1: Perform an Incremental Merge
Create a small batch of incremental updates and merge them into the target Delta table. This efficiently updates existing records and inserts new ones.
```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Create your incremental updates (e.g., 10 records with a 20% price increase)
df = spark.read.table("workspace.ecommerce.oct_events_delta")
incremental_updates = df.limit(10).withColumn("price", F.col("price") * 1.2).withColumn("updated_at", F.current_timestamp())

# Load the DeltaTable object and perform the merge
target_table = DeltaTable.forName(spark, "workspace.ecommerce.oct_events_delta")

(target_table.alias("target")
  .merge(
    incremental_updates.alias("source"),
    "target.product_id = source.product_id AND target.event_time = source.event_time"
  )
  .whenMatchedUpdate(set={"price": "source.price"})
  .whenNotMatchedInsertAll()
  .execute())

print("Incremental Merge Complete.")
```

### Step 2: Review Table History
Examine the transaction log to see a history of all operations performed on the Delta table, including the recent merge.
```sql
-- See the transaction log of everything we just did!
DESCRIBE HISTORY workspace.ecommerce.oct_events_delta
```

### Step 3: Query a Previous Version (Time Travel)
Query the data as it existed at a specific version number, demonstrating Delta Lake's powerful time travel capabilities.
```python
# See the latest version
latest_version = spark.sql("DESCRIBE HISTORY workspace.ecommerce.oct_events_delta").select("version").first().version

# Read data from one version before the latest
df_before_merge = (spark.read.format("delta")
                   .option("versionAsOf", latest_version - 1)
                   .table("workspace.ecommerce.oct_events_delta"))

print(f"Reading from version {latest_version - 1}")
display(df_before_merge)
```

### Step 4: Optimize Table Layout
Improve query performance by compacting small files into larger ones using the `OPTIMIZE` command.
```sql
OPTIMIZE workspace.ecommerce.oct_events_delta
```

### Step 5: Clean Up Stale Data Files
Remove data files that are no longer referenced by a Delta table and are older than the retention threshold using `VACUUM`.
```sql
-- VACUUM workspace.ecommerce.oct_events_delta

-- Note: By default, VACUUM prevents you from deleting files less than 7 days old.
-- This is a safety feature. To run it now, you can disable the check:
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- VACUUM workspace.ecommerce.oct_events_delta RETAIN 0 HOURS;
```
