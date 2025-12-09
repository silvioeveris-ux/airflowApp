from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extrair_dados():
    # Simula extração de dados
    print("Extraindo dados da fonte...")
    return {'compras': 200, 'artefatos': 200}

def transformar_dados(**context):
    dados = context['ti'].xcom_pull(task_ids='extrair')
    # Processa os dados
    dados['total'] = dados['compras'] + dados['artefatos']
    return dados

def carregar_dados(**context):
    dados = context['ti'].xcom_pull(task_ids='transformar')
    print(f"Carregando dados: {dados}")

with DAG(
    'MIZACK',
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/2 * * * *',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extrair', python_callable=extrair_dados)
    t2 = PythonOperator(task_id='transformar', python_callable=transformar_dados)
    t3 = PythonOperator(task_id='carregar', python_callable=carregar_dados)

    t1 >> t2 >> t3