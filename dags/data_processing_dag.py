# Removendo a adição ao PYTHONPATH pois agora a pasta app está dentro de dags
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importa as funções de processamento de dados da pasta app
from app.data_processing import (
    upload_raw_data_to_bronze,
    process_bronze_to_silver,
    process_silver_to_gold
)
from app.data_paths import setup_paths

# Define argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 10)
}

# Define a DAG
dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Pipeline para processamento de dados em camadas Bronze, Silver e Gold',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Configura os diretórios necessários
setup_paths()

# Tarefa 1: Carregar dados brutos para Bronze
raw_to_bronze = PythonOperator(
    task_id='raw_to_bronze',
    python_callable=upload_raw_data_to_bronze,
    dag=dag,
)

# Tarefa 2: Processar Bronze para Silver
bronze_to_silver = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=process_bronze_to_silver,
    dag=dag,
)

# Tarefa 3: Processar Silver para Gold
silver_to_gold = PythonOperator(
    task_id='silver_to_gold',
    python_callable=process_silver_to_gold,
    dag=dag,
)

# Define a ordem de execução das tarefas
raw_to_bronze >> bronze_to_silver >> silver_to_gold