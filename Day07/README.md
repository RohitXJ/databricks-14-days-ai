# Day 07 – Workflows & Job Orchestration

## Overview

This notebook demonstrates how to build and orchestrate a multi-task data pipeline using Databricks Workflows. It introduces notebook parameterization with widgets, allowing for dynamic and reusable jobs. The workflow automates the Medallion Architecture (Bronze -> Silver -> Gold) by defining each layer as a separate, dependent task.

![Image:](7.png)

## Operations

### Step 1: Add a Parameter Widget
To make the notebook reusable, a dropdown widget is added to select the month for data processing. This allows the same logic to be executed with different inputs without changing the code.

```python
from pyspark.sql import functions as F

# Create a dropdown widget for the month
dbutils.widgets.dropdown("month", "Oct", ["Oct", "Nov"])

# Get the value from the widget
selected_month = dbutils.widgets.get("month")

print(f"Running pipeline for month: {selected_month}")
```

### Step 2: Define Bronze, Silver, and Gold Tasks
The pipeline is broken down into three modular functions, each responsible for a specific layer of the Medallion Architecture. These functions can be run as independent tasks in a Databricks Job.

```python
# --- TASK 1: BRONZE ---
def run_bronze(month):
    raw_path = f"/Volumes/workspace/ecommerce/ecommerce_data/2019-{month}.csv"
    df = spark.read.csv(raw_path, header=True, inferSchema=True)
    df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.ecommerce.bronze_{month}")
    return f"Bronze {month} complete."

# --- TASK 2: SILVER ---
def run_silver(month):
    bronze_df = spark.read.table(f"workspace.ecommerce.bronze_{month}")
    silver_df = bronze_df.dropna(subset=["user_id"]).dropDuplicates()
    silver_df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.ecommerce.silver_{month}")
    return f"Silver {month} complete."

# --- TASK 3: GOLD ---
def run_gold(month):
    silver_df = spark.read.table(f"workspace.ecommerce.silver_{month}")
    gold_df = silver_df.groupBy("brand").agg(F.sum("price").alias("total_revenue"))
    gold_df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.ecommerce.gold_revenue_{month}")
    return f"Gold {month} complete."
```

### Step 3: Orchestrate and Execute the Workflow
In a Databricks Job, these functions would be set up as separate tasks with dependencies (e.g., `run_silver` depends on `run_bronze`). The notebook itself demonstrates a simple sequential execution. A scheduled trigger can be added to run the entire job periodically.

```python
# Execution Logic
print(run_bronze(selected_month))
print(run_silver(selected_month))
print(run_gold(selected_month))
```
