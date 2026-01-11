# Day 03 – PySpark Transformations Deep Dive

## Overview

This notebook dives deeper into PySpark transformations. It demonstrates loading and combining multiple datasets, creating derived features through joins, and performing advanced analytics using window functions to calculate running totals and time-based features.

![Image:](3.png)

## Operations

### Step 1: Load Full E-commerce Dataset
Combine the October and November 2019 e-commerce datasets.
```python
from pyspark.sql import functions as F

path_oct = "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv"
path_nov = "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv" 

df_oct = spark.read.csv(path_oct, header=True, inferSchema=True)
df_nov = spark.read.csv(path_nov, header=True, inferSchema=True)

full_data = df_oct.unionByName(df_nov)

print(f"Total Rows: {full_data.count()}")
```

### Step 2: Create Derived Features (Joins)
Identify "premium" brands (price > 1000) and enrich the main dataset with this information using a broadcast join.
```python
premium_brands = full_data.filter(F.col("price") > 1000).select("brand").distinct().withColumn("is_premium_brand", F.lit(True))

enriched_df = full_data.join(F.broadcast(premium_brands), on="brand", how="left").fillna({"is_premium_brand": False})

display(enriched_df.select("event_time", "brand", "price", "is_premium_brand").limit(10))
```

### Step 3: Calculate Running Totals (Window Functions)
Calculate the cumulative spending for each user over time using a window function partitioned by `user_id`.
```python
from pyspark.sql.window import Window

user_window = Window.partitionBy("user_id").orderBy("event_time")

final_df = enriched_df.withColumn("running_total_spend", F.sum("price").over(user_window)).withColumn("event_rank", F.row_number().over(user_window))

display(final_df.select("user_id", "event_time", "event_type", "price", "running_total_spend", "event_rank").limit(20))
```

### Step 4: Create Time-Based Features
Create a new feature to calculate the time elapsed (in seconds) between a user's consecutive events.
```python
prev_time_window = Window.partitionBy("user_id").orderBy("event_time")

feature_df = final_df.withColumn("prev_event_time", F.lag("event_time", 1).over(prev_time_window)).withColumn("seconds_since_last_event",(F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time")))

display(feature_df.select("user_id", "event_time", "event_type", "seconds_since_last_event").limit(10))
```
