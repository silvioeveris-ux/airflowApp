from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'igoranjos_ex2',
    default_args=default_args,
    description='A simple DAG that creates and transforms synthetic data',
    schedule_interval=timedelta(days=3),
    catchup=False,
)

def create_synthetic_data(**context):
    """Create synthetic customer data"""
    np.random.seed(42)
    n_records = 100

    data = {
        'customer_id': range(1, n_records + 1),
        'age': np.random.randint(18, 80, n_records),
        'purchase_amount': np.random.uniform(10, 500, n_records).round(2),
        'items_purchased': np.random.randint(1, 20, n_records),
        'region': np.random.choice(['North', 'South', 'East', 'West'], n_records),
        'customer_name': 'Igor Anjos'
    }

    df = pd.DataFrame(data)

    # Push to XCom for next task
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    print(f"Created {len(df)} synthetic records")

def transform_data(**context):
    """Transform the data using pandas"""
    # Pull data from previous task
    raw_data_json = context['ti'].xcom_pull(key='raw_data', task_ids='create_data')
    df = pd.read_json(raw_data_json)

    # Add calculated columns
    df['price_per_item'] = (df['purchase_amount'] / df['items_purchased']).round(2)
    df['customer_segment'] = pd.cut(df['age'],
                                     bins=[0, 30, 50, 100],
                                     labels=['Young', 'Middle', 'Senior'])

    # Filter high-value customers
    df['is_high_value'] = df['purchase_amount'] > 200

    # Push transformed data
    context['ti'].xcom_push(key='transformed_data', value=df.to_json())
    print(f"Transformed data with {len(df)} records")

def analyze_data(**context):
    """Analyze the transformed data"""
    transformed_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.read_json(transformed_json)

    # Calculate summary statistics
    summary = {
        'total_customers': len(df),
        'avg_purchase_amount': df['purchase_amount'].mean().round(2),
        'high_value_customers': df['is_high_value'].sum(),
        'customers_by_region': df['region'].value_counts().to_dict(),
    }

    print("Analysis Summary:")
    for key, value in summary.items():
        print(f"{key}: {value}")
    df.to_csv('/tmp/igoranjos2.csv')

# Define tasks
create_data_task = PythonOperator(
    task_id='create_data',
    python_callable=create_synthetic_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

# Set task dependencies
create_data_task >> transform_data_task >> analyze_data_task