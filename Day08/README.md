# Day 08 – Unity Catalog Governance

## Overview

This notebook focuses on implementing data governance using Databricks Unity Catalog. It demonstrates how to create and manage catalogs, schemas, and tables, and then secure them by setting fine-grained permissions. The notebook also covers creating secure views to enforce row-level and column-level security, ensuring users can only access the data they are authorized to see.

![Image:](8.png)

## Operations

### Step 1: Create Schema and Managed Table
First, a new schema and a managed Delta table are created to store sensor data. Unity Catalog manages the lifecycle and file paths for managed tables, simplifying data organization.

```sql
-- Set the context
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS governance_lab;
USE SCHEMA governance_lab;

-- Create a MANAGED Delta Table
CREATE TABLE IF NOT EXISTS managed_sensor_data (
    sensor_id INT,
    temperature DOUBLE,
    timestamp TIMESTAMP
) USING DELTA;
```

### Step 2: Manage Table Permissions
Unity Catalog allows for precise access control using SQL `GRANT` and `REVOKE` statements. Here, read-only access is granted to a general `users` group, while all privileges are revoked from a hypothetical `intern_account`.

```sql
-- Grant read-only access to a specific group/user
GRANT USAGE ON SCHEMA governance_lab TO `users`;
GRANT SELECT ON TABLE managed_sensor_data TO `users`;

-- Revoke permissions if a user changes roles
REVOKE ALL PRIVILEGES ON TABLE managed_sensor_data FROM `intern_account`;

-- Check who has access
SHOW GRANTS ON TABLE managed_sensor_data;
```

### Step 3: Create a Secure View for Controlled Access
To implement row-level security, a view is created that only exposes a subset of the data. In this case, the `critical_alerts` view shows only sensor readings where the temperature exceeds 100. Unity Catalog automatically tracks the lineage from the view back to the base table.

```sql
-- Create a View that filters sensitive data
CREATE OR REPLACE VIEW critical_alerts AS
SELECT sensor_id, temperature
FROM managed_sensor_data
WHERE temperature > 100;
```

### Step 4: Verify Access and Lineage
Finally, the notebook verifies that the permissions are correctly applied by querying the secure view. Attempting to access the view from a user with the correct grants will succeed, while an unauthorized user would be denied.

```python
import pandas as pd

# Define the 3-level path to the secure view
table_path = "workspace.governance_lab.critical_alerts"

try:
    # Attempt to read the view into a DataFrame
    df = spark.sql(f"SELECT * FROM {table_path}").toPandas()
    print("Governance Lab Success: View loaded into Pandas.")
    print(df.head())
except Exception as e:
    print(f"Access Denied or Table Missing: {e}")
```
