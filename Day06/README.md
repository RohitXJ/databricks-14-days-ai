# Day 06 – Medallion Architecture

## Overview

This notebook implements the Medallion Architecture, a data quality framework used to logically organize data in a lakehouse. It demonstrates the creation of three layers: Bronze (raw data), Silver (cleaned and validated data), and Gold (business-level aggregates).

![Image:](6.png)

## Operations

### Step 1: Build the Bronze Layer (Raw Ingestion)
This first layer ingests the raw, unaltered data from the source system (a CSV file in this case) and stores it in the Delta format. This provides a durable, queryable source of truth.
```python
from pyspark.sql import functions as F

# Path to raw file
raw_path = "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv"

# BRONZE: Raw Ingestion
# We keep all columns, even the ones with nulls
bronze_df = spark.read.csv(raw_path, header=True, inferSchema=True)

# Save to Bronze Table
bronze_df.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.bronze_events")

print("Bronze Layer: Raw data ingested.")
```

### Step 2: Build the Silver Layer (Cleaning & Validation)
The silver layer takes the raw data from the bronze layer and applies cleaning, validation, and enrichment. This includes handling nulls, correcting data types, and removing duplicates to create a reliable, query-ready dataset.
```python
# Load from Bronze
bronze_data = spark.read.table("workspace.ecommerce.bronze_events")

# SILVER: Cleaning
# 1. Drop rows with no user_id or product_id
# 2. Convert event_time to actual Timestamp type
# 3. Fill missing category_code and brand
silver_df = bronze_data.dropna(subset=["user_id", "product_id"]) \
    .withColumn("event_time", F.to_timestamp("event_time")) \
    .fillna({"category_code": "Unknown", "brand": "Generic"}) \
    .dropDuplicates()

# Save to Silver Table
silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.silver_events")

print("Silver Layer: Data cleaned and validated.")
```

### Step 3: Build the Gold Layer (Business Aggregates)
The gold layer contains highly refined, business-level aggregated data ready for analytics and reporting. This example creates a table summarizing daily revenue and sales counts per brand.
```python
# Load from Silver
silver_data = spark.read.table("workspace.ecommerce.silver_events")

# GOLD: Aggregations (e.g., Daily Revenue per Brand)
gold_df = silver_data.filter(F.col("event_type") == "purchase") \
    .groupBy(F.window("event_time", "1 day"), "brand") \
    .agg(
        F.sum("price").alias("daily_revenue"),
        F.count("product_id").alias("total_sales")
    ) \
    .orderBy(F.desc("daily_revenue"))

# Save to Gold Table
gold_df.write.format("delta").mode("overwrite").saveAsTable("workspace.ecommerce.gold_brand_performance")

print("Gold Layer: Business aggregates built.")
display(gold_df.limit(10))
```
