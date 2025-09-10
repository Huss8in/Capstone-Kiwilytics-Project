from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import numpy as np

PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DAILY_REVENUE_QUERY = """
    SELECT 
        o.orderdate::date as sale_date,
        SUM(p.price * od.quantity) AS daily_revenue
    FROM orders o
    JOIN order_details od ON o.orderid = od.orderid
    JOIN products p ON od.productid = p.productid
    GROUP BY o.orderdate::date
    ORDER BY sale_date;
"""

def extract_sales_data(**context):
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    df = postgres_hook.get_pandas_df(DAILY_REVENUE_QUERY)
    print(f"Extracted {len(df)} days of sales data")
    return df.to_dict('records')

def calculate_daily_revenue(**context):
    daily_data = context['task_instance'].xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(daily_data)
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df = df.sort_values('sale_date')
    
    total_revenue = df['daily_revenue'].sum()
    avg_daily_revenue = df['daily_revenue'].mean()
    
    print(f"Total Revenue: ${total_revenue:,.2f}")
    print(f"Average Daily Revenue: ${avg_daily_revenue:,.2f}")
    print(f"Days analyzed: {len(df)}")
    
    return df.to_dict('records')

def create_visualization(**context):
    data = context['task_instance'].xcom_pull(task_ids='calculate_revenue')
    df = pd.DataFrame(data)
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    
    plt.figure(figsize=(12, 6))
    plt.plot(df['sale_date'], df['daily_revenue'], marker='o', linewidth=2)
    plt.title('Daily Sales Revenue')
    plt.xlabel('Date')
    plt.ylabel('Revenue ($)')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plot_path = '/tmp/daily_revenue_plot.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Visualization saved to: {plot_path}")
    return plot_path

with DAG(
    dag_id='sales_revenue_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='Daily sales revenue analysis pipeline',
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_sales_data
    )

    calculate_task = PythonOperator(
        task_id='calculate_revenue',
        python_callable=calculate_daily_revenue
    )

    visualize_task = PythonOperator(
        task_id='create_visualization',
        python_callable=create_visualization
    )

    extract_task >> calculate_task >> visualize_task