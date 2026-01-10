# Day 02 – Apache Spark Fundamentals

## Overview

This notebook demonstrates fundamental Spark operations on the E-commerce dataset. It covers reading data, selecting, filtering, grouping, ordering, and exporting the results.

![Image:](2.png)

## Operations

### Step 1: Load Data
Read the October 2019 e-commerce dataset into a Spark DataFrame.
```python
file_path = "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv"

oct_events = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)
display(oct_events.limit(10))
```

### Step 2: Select Columns
Select the `category_code` and `price` columns from the DataFrame.
```python
from pyspark.sql.functions import col, desc

selected_df = oct_events.select("category_code", "category_code", "price")
display(selected_df.limit(10))
```

### Step 3: Filter Data
Filter the DataFrame to include only products with a price greater than 300.
```python
filtered_df = selected_df.filter(col("Price") > 300)
display(filtered_df.limit(10))
```

### Step 4: Group and Aggregate
Group the data by `category_code` and count the number of products in each category.
```python
grouped_df = filtered_df.groupBy("category_code").count()
display(grouped_df.limit(10))
```

### Step 5: Order Results
Order the categories by the product count in descending order.
```python
final_df = grouped_df.orderBy(desc("count"))
final_df.show()
```

### Step 6: Export Results
Save the processed DataFrame to a CSV file.
```python
final_df.write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct_Processed_Product_Centric_Result.csv")

print("Export Complete!")
```
