# Day 0 – Setup & Data Loading (Prerequisites)

## Overview

This notebook outlines the setup process for an E-commerce dataset using Apache Spark. It includes configuration steps, data ingestion, and initial exploratory data analysis.

![Image:](14_Days_Databricks_daily_Challenge_tasks.webp)

## Prerequisites

Before proceeding, ensure you have the following:

1. **Apache Spark**: Install Apache Spark on your system.
2. **Kaggle API Credentials**: Set up Kaggle credentials in your environment.

## Configuration Steps

### Step 1: Install Necessary Packages
```bash
pip install kaggle
```

### Step 2: Configure Kaggle Environment Variables
Set the following environment variables to authenticate with Kaggle:
```python
os.environ["KAGGLE_USERNAME"] = "YOUR_KAGGLE_USERNAME"
os.environ["KAGGLE_KEY"] = "YOUR_KAGGLE_KEY"
```

### Step 3: Create Spark Schema and Volume
Create a schema and volume for the E-commerce dataset:
```sql
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce;
CREATE VOLUME IF NOT EXISTS workspace.ecommerce.ecommerce_data;
```

## Data Loading

The notebook proceeds to load data into Apache Spark:

### Step 4: Download Dataset from Kaggle
Download the E-commerce behavior dataset from Kaggle:
```bash
cd /Volumes/workspace/ecommerce/ecommerce_data
kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store
```

### Step 5: Unzip and List Files
Unzip the downloaded file and list its contents:
```bash
cd /Volumes/workspace/ecommerce/ecommerce_data
unzip -o ecommerce-behavior-data-from-multi-category-store.zip
ls -lh

# Remove zip file to save space
rm -f ecommerce-behavior-data-from-multi-category-store.zip
ls -lh
```

### Step 6: Load Data into Spark
Load the dataset for October and November 2019:
```python
oct_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",
    header=True,
    inferSchema=True
)
nov_events = spark.read.csv(
    "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",
    header=True,
    inferSchema=True
)

# Display first 10 rows of November events
display(nov_events.limit(10))
```

### Step 7: Print Event Counts and Schema
Print the total number of events for October:
```python
print(f"October 2019 - Total Events: {oct_events.count():,}")
```

Print the schema of the `nov_events` DataFrame:
```python
print("\n" + "="*60)
print("SCHEMA:")
print("="*60)
nov_events.printSchema()
```

### Step 8: Display Sample Data
Show the first 5 rows of November events with a truncated display:
```python
print("\n" + "="*60)
print("SAMPLE DATA (First 5 rows):")
print("="*60)
nov_events.show(5, truncate=False)
```

## Next Steps

The notebook concludes by deleting previously loaded DataFrames and demonstrates creating a simple DataFrame:

### Step 9: Create Simple DataFrame
Create a DataFrame with product names and prices:
```python
data = [("iPhone", 999), ("Samsung", 799), ("MacBook", 1299)]
df = spark.createDataFrame(data, ["product", "price"])
df.show()
```

### Step 10: Filter Expensive Products
Filter the DataFrame to display only expensive products:
```python
df.filter(df.price > 1000).show()
```

This summary provides an overview of the setup and data loading process outlined in the notebook.