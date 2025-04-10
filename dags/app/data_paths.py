"""
Definição de caminhos para arquivos de dados.
"""
from pathlib import Path
import os

# Define o diretório base do projeto
# Determine base directory based on environment
if os.environ.get('AIRFLOW_HOME'):
    # Em ambiente Airflow
    BASE_DIR = Path(os.environ.get('AIRFLOW_HOME', '/opt/airflow'))
else:
    # Em ambiente local - usa o diretório pai do arquivo atual
    BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = BASE_DIR / 'data/0_raw/raw_data.csv'
BRONZE_PATH = BASE_DIR / 'data/1_bronze/bronze.csv'
SILVER_PATH = BASE_DIR / 'data/2_silver/silver.csv'
GOLD_PATH = BASE_DIR / 'data/3_gold/gold.csv'

def setup_paths():
    """
    Certifica-se de que os diretórios necessários existem.
    """
    os.makedirs(BASE_DIR / 'data/1_bronze', exist_ok=True)
    os.makedirs(BASE_DIR / 'data/2_silver', exist_ok=True)
    os.makedirs(BASE_DIR / 'data/3_gold', exist_ok=True)