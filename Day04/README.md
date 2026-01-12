# Day 04 – Delta Lake Introduction

## Overview

This notebook introduces the core concepts of Delta Lake. It covers how to convert existing data from CSV to the Delta format, create managed Delta tables, and leverage key features like schema enforcement and ACID transactions through merge operations.

![Image:](4.png)

## Operations

### Step 1: Prepare Schema and Volume
Set up the necessary catalog, schema, and volume in Databricks to store Delta files.
```sql
-- Ensure we are in the right context
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Create the Volume where your Delta FILES will live
CREATE VOLUME IF NOT EXISTS workspace.ecommerce.delta_storage;
```

### Step 2: Convert CSV to Delta Format
Load a raw CSV file, write it to a Delta Lake path, and register it as a managed table.
```python
# Load raw October data
path_oct = "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv"
df = spark.read.csv(path_oct, header=True, inferSchema=True)

# Define the Delta path inside your Volume
delta_volume_path = "/Volumes/workspace/ecommerce/delta_storage/ecommerce_events_files"

# Write as Delta files
df.write.format("delta").mode("overwrite").save(delta_volume_path)

# Register as a Managed Table (Best for SQL/Analytics)
df.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.oct_events_delta")

print("Successfully converted CSV to Delta and registered the table!")
```

### Step 3: Test Schema Enforcement
Demonstrate how Delta Lake prevents writing data that does not conform to the table's schema.
```python
from pyspark.sql import Row

# Create a row with an extra column 'coupon_code' that doesn't exist in the table
bad_data = [Row(event_time="2019-10-01", event_type="view", product_id=123, 
                category_id=456, category_code="electronics", brand="apple", 
                price=999.0, user_id=789, user_session="abc", coupon_code="DISCOUNT10")]

bad_df = spark.createDataFrame(bad_data)

try:
    # This should fail because 'coupon_code' is not in the schema
    bad_df.write.format("delta").mode("append").saveAsTable("workspace.ecommerce.oct_events_delta")
except Exception as e:
    print("-" * 30)
    print("✅ SCHEMA ENFORCEMENT TEST SUCCESSFUL")
    print("The system blocked the write as expected. Error caught!")
    print("-" * 30)
```

### Step 4: Handle Upserts with `MERGE`
Use the `MERGE` operation to efficiently update existing records and insert new ones (upsert), preventing duplicates.
```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# 1. Access the Delta Table
deltaTable = DeltaTable.forName(spark, "workspace.ecommerce.oct_events_delta")

# 2. Create a "Batch" of updates (simulating 5 rows with a price change)
updates_df = df.limit(5).withColumn("price", col("price") + 5.0)

# 3. Perform the Merge logic
(deltaTable.alias("target")
  .merge(
    updates_df.alias("source"),
    "target.product_id = source.product_id AND target.event_time = source.event_time"
  )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute())

print("MERGE (Upsert) operation completed successfully!")
```

### Step 5: Review Table History
Examine the transaction log to see a history of all operations performed on the Delta table.
```sql
-- See the transaction log of everything we just did!
DESCRIBE HISTORY workspace.ecommerce.oct_events_delta;
```
