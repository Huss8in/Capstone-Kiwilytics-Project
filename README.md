# Capstone-Kiwilytics-Project

## Overview
This project demonstrates an **end-to-end automated data pipeline** built with **Apache Airflow**. It extracts sales data from a PostgreSQL database, calculates daily revenue, and generates a time series visualization of revenue trends.

The project highlights skills in **ETL automation, Python programming, SQL, and data visualization**, making it a strong portfolio example for data engineering and analytics workflows.

## Pipeline Workflow
The Airflow DAG (`sales_revenue_pipeline`) includes three main tasks:

1. **Extract Data:** Pulls daily sales data by joining `orders`, `order_details`, and `products`.  
2. **Calculate Revenue:** Computes total and average daily revenue, sorting data by date.  
3. **Visualize Revenue:** Generates a line plot of daily sales revenue and saves it as a PNG file.

## Tools & Technologies
- Python  
- Apache Airflow  
- PostgreSQL  
- Pandas  
- Matplotlib  

## How to Run
1. Configure your Airflow environment and add a Postgres connection (`postgres_conn`).  
2. Place the DAG file in the Airflow `dags/` directory.  
3. Start Airflow and trigger the `sales_revenue_pipeline` DAG.  
4. Check the `/tmp/daily_revenue_plot.png` for the generated visualization.

## Outcome
- Automated daily extraction, computation, and visualization of sales revenue.  
- Clear insights into revenue trends and total daily sales.  

## Visualization
<img width="4170" height="2375" alt="image" src="https://github.com/user-attachments/assets/9d67e65f-d6d1-490e-890a-0889901afd4b" />

