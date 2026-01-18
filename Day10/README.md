# Day 10 – Performance Optimization

## Overview

This notebook focuses on the "Engine Room" of the Databricks Lakehouse: Performance Optimization. It demonstrates how to transition from simply writing functional queries to architecting high-performance data layouts. By leveraging a 1-million-row dataset, this lab explores the impact of Execution Plans, Data Partitioning, and Z-Ordering. The goal is to master "Data Skipping"—the ability for the Spark engine to ignore irrelevant files to drastically reduce query latency and compute costs.

![Image:](10.png)

## Operations

### Step 1: Create a Large-Scale Dataset

To observe meaningful performance differences, a schema and a large table named `raw_sales_large` are created. This table contains 1,000,000 rows of synthetic sales data. Note the explicit `CAST` to `INT` to ensure compatibility with Spark date functions.

```sql
-- Set the environment context
USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS performance_lab;
USE SCHEMA performance_lab;

-- Create a large "unoptimized" table (1 Million Rows)
CREATE OR REPLACE TABLE raw_sales_large AS
SELECT 
    id AS order_id,
    date_add(current_date(), CAST(-(id % 365) AS INT)) AS order_date,
    (id % 1000) AS product_id,
    CASE 
        WHEN id % 4 = 0 THEN 'Electronics'
        WHEN id % 4 = 1 THEN 'Fashion'
        WHEN id % 4 = 2 THEN 'Home'
        ELSE 'Beauty'
    END AS category,
    (id * 1.5 % 100) AS amount
FROM range(1, 1000001);
```

### Step 2: Analyze Query Execution Plans

Before optimizing, we use the `EXPLAIN` command to see the "Physical Plan." This allows us to identify if the engine is performing a costly full file scan (reading the entire 1M row dataset) or if it is successfully skipping data.

```sql
-- Analyze the plan to identify potential bottlenecks
EXPLAIN SELECT * FROM raw_sales_large 
WHERE product_id = 500 AND category = 'Electronics';
```

### Step 3: Implement Partitioning and Z-Ordering

We apply industry-standard optimization techniques to physically reorganize the data. Partitioning is used for low-cardinality columns (Category) to create a folder-based hierarchy, while Z-Ordering is used for high-cardinality columns (Product ID) to sort data within files for multidimensional skipping.

```sql
-- Create a partitioned version of the table
CREATE OR REPLACE TABLE partitioned_sales
PARTITIONED BY (category)
AS SELECT * FROM raw_sales_large;

-- Apply Z-Ordering for multi-dimensional data skipping
OPTIMIZE partitioned_sales 
ZORDER BY (product_id);
```

### Step 4: Benchmark Performance Improvements

Finally, we run a side-by-side comparison between the unoptimized and optimized tables. By monitoring the "Duration" and the "Number of Files Scanned" in the Spark UI, we quantify the massive efficiency gains achieved through proper data engineering.

```sql
-- Query A: The Unoptimized Table (Full Scan)
SELECT 
    'Unoptimized' AS table_type, 
    SUM(amount) AS total_sales
FROM raw_sales_large 
WHERE product_id = 500 AND category = 'Electronics';

-- Query B: The Optimized Table (Data Skipping)
SELECT 
    'Optimized' AS table_type, 
    SUM(amount) AS total_sales
FROM partitioned_sales 
WHERE product_id = 500 AND category = 'Electronics';
```