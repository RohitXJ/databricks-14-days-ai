# Day 12 – MLflow Basics

## Overview

This notebook introduces the fundamentals of MLflow for tracking machine learning experiments. It demonstrates how to train a simple regression model, log its parameters, metrics, and the model artifact to MLflow, and then review the experiment results. This is a foundational step for managing the ML lifecycle and ensuring reproducibility.

![Image:](12.png)

## Operations

### Step 1: Load and Prepare Data

A sample of the e-commerce dataset is loaded into a Pandas DataFrame for quick processing. The data is preprocessed to handle numeric conversions and create a binary target variable (`event_type_encoded`). Finally, it is split into training and testing sets.

```python
import pandas as pd
from sklearn.model_selection import train_test_split

# Load only a sample to keep it fast and avoid memory issues
raw_df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv", header=True, inferSchema=True) \
    .limit(100000) \
    .toPandas()

# Quick preprocessing in Pandas for ML
df = raw_df.copy()
df["price"] = pd.to_numeric(df["price"], errors='coerce').fillna(0)
df["event_type_encoded"] = (df["event_type"] == 'purchase').astype(int)

# Use 'price' as the feature to predict 'purchase' intent
X = df[['price']]
y = df['event_type_encoded']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
```

### Step 2: Train Model and Track with MLflow

An MLflow run is initiated to track the experiment. Within the run, we log key-value parameters like the data source and sample size. A simple `LinearRegression` model is trained, evaluated using Mean Squared Error (MSE), and the resulting metric is logged. The trained model itself is also saved using `mlflow.sklearn.log_model`.

```python
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

with mlflow.start_run(run_name="fast_csv_sample_v1"):
    # Log metadata
    mlflow.log_param("data_source", "2019-Nov.csv_sample")
    mlflow.log_param("sample_size", 100000)
    
    # Train
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Evaluate
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    mlflow.log_metric("mse", mse)
    
    # Save model to MLflow
    mlflow.sklearn.log_model(model, "regression_model")
    
    print(f"Model trained and logged. MSE: {mse:.6f}")
```

### Step 3: View and Compare Runs

To programmatically review and compare experiments, `mlflow.search_runs` is used. This function retrieves a DataFrame containing details of recent runs, allowing for easy comparison of parameters and metrics, which is crucial for model selection and performance analysis.

```python
# List the last few runs to confirm logging
recent_runs = mlflow.search_runs(max_results=3)
display(recent_runs[['run_id', 'params.data_source', 'metrics.mse']])
```

