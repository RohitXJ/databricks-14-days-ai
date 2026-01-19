# Day 11 – Statistical Analysis & ML Prep

## Overview

This notebook dives into statistical analysis and machine learning preparation using PySpark. It demonstrates how to derive insights from raw data and engineer features for predictive modeling. Using a large e-commerce dataset, this lab covers calculating descriptive statistics, testing hypotheses about user behavior, identifying correlations, and creating temporal and behavioral features for an ML model.

![Image:](11.png)

## Operations

### Step 1: Load and Prepare Data

The initial step involves loading the e-commerce event data from a CSV file. The `event_time` string is converted to a proper timestamp format, and an `event_date` column is extracted to facilitate time-based analysis.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load data and convert event_time string to timestamp for analysis
events = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True) \
    .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")) \
    .withColumn("event_date", F.to_date("event_time"))
```

### Step 2: Calculate Statistical Summaries

To understand the distribution of product prices, we calculate descriptive statistics (count, mean, standard deviation, min, and max) for the `price` column. This helps identify the data's central tendency, spread, and potential outliers.

```python
# Calculate statistical summaries for the price column
events.describe(["price"]).show()
```

### Step 3: Test Hypotheses (Weekday vs. Weekend)

A common business question is whether user behavior differs between weekdays and weekends. We test this by creating a boolean flag `is_weekend` and then grouping by this flag and `event_type` to observe any significant variations in user actions like views, cart additions, and purchases.

```python
# Test if user behavior changes on weekends (1=Sunday, 7=Saturday)
weekday = events.withColumn("is_weekend", F.dayofweek("event_date").isin([1, 7]))
weekday.groupBy("is_weekend", "event_type").count().show()
```

### Step 4: Identify Correlations

We investigate the relationship between product price and the likelihood of a purchase. By creating a binary `is_purchase` column, we can calculate the correlation coefficient between `price` and `is_purchase` to determine if there's a statistical link.

```python
# Identify if higher prices correlate with fewer purchases
corr_df = events.withColumn("is_purchase", F.when(F.col("event_type") == "purchase", 1).otherwise(0))
correlation = corr_df.stat.corr("price", "is_purchase")
print(f"Correlation between Price and Purchase: {correlation}")
```

### Step 5: Engineer Features for ML

Feature engineering is a critical step for building effective machine learning models. Here, we create several new features:
- **Temporal Features:** `hour` of the day and `day_of_week`.
- **Behavioral Feature:** `time_since_first_view` calculates the duration since a user's first interaction.
- **Transformation:** A log transformation (`price_log`) is applied to the price to handle its skewed distribution.

```python
# Engineer temporal and behavioral features for model training
features = events.withColumn("hour", F.hour("event_time")) \
    .withColumn("day_of_week", F.dayofweek("event_date")) \
    .withColumn("price_log", F.log(F.col("price") + 1)) \
    .withColumn("time_since_first_view", 
        F.unix_timestamp("event_time") - 
        F.unix_timestamp(F.first("event_time").over(Window.partitionBy("user_id").orderBy("event_time"))))

features.select("user_id", "hour", "day_of_week", "price_log", "time_since_first_view").show(5)
```

### Step 6: Save Engineered Features

Finally, the enriched dataset containing the newly engineered features is saved as a Delta table. This table is now ready to be used for training a machine learning model.

```python
# Create the schema if it was missed in this session and save the table
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ml_prep_lab")

features.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ml_prep_lab.engineered_features")

print("Success: Data saved to workspace.ml_prep_lab.engineered_features")
```
