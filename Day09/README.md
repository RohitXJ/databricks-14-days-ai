# Day 09 – SQL Analytics & Dashboards

## Overview

This notebook focuses on leveraging Databricks SQL for data analytics. It demonstrates the process of creating a mock sales dataset and then executing a series of analytical queries to uncover business insights. The queries explore daily revenue trends with moving averages, rank product categories by performance, and build a sales funnel to analyze user conversion. These are the foundational steps for building interactive dashboards.

![Image:](9.png)

## Operations

### Step 1: Create Sample Dataset
First, a new schema and a table are created to store mock sales data. This table, `sales_data`, is populated with 500 rows of synthetic data, including order details, categories, and sales amounts, to simulate a realistic e-commerce scenario.

```sql
-- Set the environment
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS sql_analytics_lab;
USE SCHEMA sql_analytics_lab;

-- Create Mock Sales Data
CREATE OR REPLACE TABLE sales_data AS
SELECT 
    id as order_id,
    date_add(current_date(), CAST(-(id % 30) AS INT)) as order_date,
    (id * 123 % 500) as customer_id,
    CASE 
        WHEN id % 3 = 0 THEN 'Electronics'
        WHEN id % 3 = 1 THEN 'Fashion'
        ELSE 'Home & Garden'
    END as category,
    (id * 77 % 1000) as sale_amount,
    CASE 
        WHEN id % 10 < 7 THEN 'Completed'
        WHEN id % 10 < 9 THEN 'Pending'
        ELSE 'Cancelled'
    END as status
FROM range(1, 501);
```

### Step 2: Perform Analytical Queries
With the data in place, several advanced SQL queries are executed to derive business intelligence. These queries are designed to be used as visualizations in a Databricks SQL Dashboard.

#### Query 1: Daily Revenue Trends
This query calculates the total daily revenue from completed sales and computes a 7-day moving average to smooth out daily fluctuations and identify trends.

```sql
-- Revenue Trends (Daily Aggregation)
SELECT 
    order_date, 
    SUM(sale_amount) as daily_revenue,
    AVG(SUM(sale_amount)) OVER(ORDER BY order_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as moving_avg_7d
FROM sales_data
WHERE status = 'Completed'
GROUP BY order_date
ORDER BY order_date;
```

#### Query 2: Top Performing Categories
This query uses a window function (`RANK()`) to determine the top-performing product categories based on total revenue, helping to identify which categories are driving the most sales.

```sql
-- Top Performing Categories (Window Function)
SELECT 
    category,
    SUM(sale_amount) as total_revenue,
    RANK() OVER (ORDER BY SUM(sale_amount) DESC) as category_rank
FROM sales_data
GROUP BY category;
```

#### Query 3: Sales Funnel Analysis
This query constructs a sales funnel to track customer engagement and conversion. It calculates the number of total, engaged (non-cancelled), and converted (completed) users.

```sql
-- Sales Funnel (Conversion Logic)
WITH funnel_base AS (
    SELECT 
        count(distinct customer_id) as total_users,
        count(distinct CASE WHEN status != 'Cancelled' THEN customer_id END) as engaged_users,
        count(distinct CASE WHEN status = 'Completed' THEN customer_id END) as converted_users
    FROM sales_data
)
SELECT 'Total Users' as stage, total_users as count FROM funnel_base
UNION ALL
SELECT 'Engaged Users', engaged_users FROM funnel_base
UNION ALL
SELECT 'Converted Users', converted_users FROM funnel_base;
```
