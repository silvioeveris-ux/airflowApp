from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
#from pymongo import MongoClient
import json

def read_csv(**context):
    # Read CSV file
    df = pd.read_csv('/opt/airflow/bd1.csv')

    # Select first 5 rows and specific columns
    data = df[['customer_id', 'age', 'purchase_amount']].head(5)

    # Convert to list of dictionaries
    records = data.to_dict('records')

    # Push to XCom for other tasks to use
    context['ti'].xcom_push(key='records', value=records)
    print(f"Read {len(records)} records from CSV")

def insert_row(row_index, **context):
    # Pull data from XCom
    records = context['ti'].xcom_pull(task_ids='read_csv_task', key='records')
    record = records[row_index]
    with open(str(row_index)+'.json', 'w') as f:
        json.dump(record, f)


# Define DAG
with DAG(
    dag_id='igoranjos_ex3',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['csv', 'mongodb', 'parallel']
) as dag:

    # Task 1: Read CSV
    read_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv
    )

    # Tasks 2-6: Insert each row
    insert_tasks = []
    for i in range(10):
        insert_task = PythonOperator(
            task_id=f'insert_row_{i}',
            python_callable=insert_row,
            op_kwargs={'row_index': i}
        )
        insert_tasks.append(insert_task)

    # Set dependencies: read first, then all inserts in parallel
    read_task >> insert_tasks