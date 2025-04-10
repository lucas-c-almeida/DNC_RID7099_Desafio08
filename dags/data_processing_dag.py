from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import re
from pathlib import Path

# Define o diretório base do projeto
BASE_DIR = Path('/opt/airflow')
RAW_DATA_PATH = BASE_DIR / 'data/0_raw/raw_data.csv'
BRONZE_PATH = BASE_DIR / 'data/1_bronze/bronze.csv'
SILVER_PATH = BASE_DIR / 'data/2_silver/silver.csv'
GOLD_PATH = BASE_DIR / 'data/3_gold/gold.csv'

# Certifica-se de que os diretórios existem
os.makedirs(BASE_DIR / 'data/1_bronze', exist_ok=True)
os.makedirs(BASE_DIR / 'data/2_silver', exist_ok=True)
os.makedirs(BASE_DIR / 'data/3_gold', exist_ok=True)

# Função para carregar dados brutos para a camada Bronze
def upload_raw_data_to_bronze():
    print("Iniciando carregamento de dados brutos para Bronze...")
    try:
        # Lê o arquivo CSV 
        df = pd.read_csv(RAW_DATA_PATH)
        # Salva o dataframe na camada Bronze
        df.to_csv(BRONZE_PATH, index=False)
        print(f"Dados carregados com sucesso para {BRONZE_PATH}")
        return True
    except Exception as e:
        print(f"Erro ao carregar dados para Bronze: {str(e)}")
        raise e

# Funções auxiliares para a etapa Bronze para Silver
def name_preprocess(name):
    """
    Limpa o nome removendo espaços extras e convertendo para minúsculas.
    """
    if isinstance(name, str):
        name = name.strip().lower()
        return name
    return None  # Trata entradas não string

def date_preprocess(date):
    """
    Converte strings de data para objetos datetime.
    """
    if isinstance(date, pd.Timestamp):
        return date
    
    if isinstance(date, str):
        try:
            return pd.to_datetime(date, errors='coerce')
        except ValueError:
            return None  # Trata formatos de data inválidos
        
    return None  # Trata entradas não string

def email_at_fill(email):
    """
    Corrige emails adicionando "@" antes de "example", "gmail", etc., se estiver faltando.
    """
    if isinstance(email, str):
        domains = ["example", "gmail", "hotmail", "outlook", "yahoo"]
        for domain in domains:
            if domain in email and "@" not in email:
                email = email.replace(domain, "@" + domain)
        return email
    return email 

def email_dotcom_fill(email):
    """
    Adiciona '.com' a emails sem um final de domínio válido.
    """
    if isinstance(email, str):
        if not re.search(r"\.[a-zA-Z]{2,}$", email):
            email = email.strip() + ".com"
        return email
    return email

def validate_email(email):
    """
    Valida o formato do email usando regex.
    """
    if isinstance(email, str):
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        email_match = re.match(pattern, email)
        if email_match:
            return email_match.group(0)
    return None

def email_preprocess(email):
    if isinstance(email, str):
        email = email.strip().lower()
        email = email_at_fill(email)
        email = email_dotcom_fill(email)
        email = validate_email(email)
    return email

def merge_duplicate_emails(df):
    """
    Combina registros com emails duplicados, preenchendo dados ausentes dos duplicados.
    """
    duplicate_emails = df[df['email'].duplicated(keep=False)]

    for email in duplicate_emails['email'].unique():
        email_group = df[df['email'] == email]

        if len(email_group) > 1:
            first_index = email_group.index[0]

            for index in email_group.index[1:]:
                for col in df.columns:
                    if pd.isna(df.loc[first_index, col]) and not pd.isna(df.loc[index, col]):
                        df.loc[first_index, col] = df.loc[index, col]

    df = df.drop_duplicates(subset=['email'], keep='first')
    return df

def calculate_age(date_of_birth):
    """
    Calcula a idade a partir da data de nascimento.
    """
    if pd.isna(date_of_birth):
        return None
    
    today = datetime.now().date()
    birth_date = date_of_birth.date()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age

# Função para processar dados da Bronze para a camada Silver
def process_bronze_to_silver():
    print("Iniciando processamento de Bronze para Silver...")
    try:
        # Lê o arquivo CSV da camada Bronze
        df = pd.read_csv(BRONZE_PATH)
        
        # Processamento de dados
        # Tratamento de colunas
        df['email'] = df['email'].apply(email_preprocess)
        df['name'] = df['name'].apply(name_preprocess)
        df['date_of_birth'] = df['date_of_birth'].apply(date_preprocess)
        df['signup_date'] = df['signup_date'].apply(date_preprocess)
        
        # Combinar emails duplicados
        df = merge_duplicate_emails(df)
        
        # Remover linhas com emails inválidos ou nomes nulos ou datas de nascimento nulas
        df = df.dropna(subset=['email', 'name', 'date_of_birth'])
        
        # Calcular a idade dos usuários
        df['age'] = df['date_of_birth'].apply(calculate_age)
        
        # Ordenar por data de cadastro
        df = df.sort_values(by='signup_date')
        
        # Salva o dataframe limpo na camada Silver
        df.to_csv(SILVER_PATH, index=False)
        print(f"Dados processados com sucesso para {SILVER_PATH}")
        return True
    except Exception as e:
        print(f"Erro ao processar dados para Silver: {str(e)}")
        raise e

# Função para processar dados da Silver para a camada Gold
def process_silver_to_gold():
    print("Iniciando processamento de Silver para Gold...")
    try:
        # Lê o arquivo CSV da camada Silver
        df = pd.read_csv(SILVER_PATH)
        
        # Definir faixas etárias
        age_bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
        age_labels = ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90', '91-100', '101+']
        
        # Categorizar idades em faixas etárias
        df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels, right=False)
        
        # Agregação por faixa etária e status (active ou inactive)
        agg_df = df.groupby(['age_group', 'status']).size().reset_index(name='count')
        
        # Pivotear a tabela para ter faixas etárias nas linhas e status nas colunas
        pivot_df = agg_df.pivot_table(index='age_group', columns='status', values='count', fill_value=0).reset_index()
        
        # Garantir que todas as colunas de status existem
        if 'active' not in pivot_df.columns:
            pivot_df['active'] = 0
        if 'inactive' not in pivot_df.columns:
            pivot_df['inactive'] = 0
        
        # Calcular o total de usuários por faixa etária
        pivot_df['total'] = pivot_df['active'] + pivot_df['inactive']
        
        # Salva o dataframe agregado na camada Gold
        pivot_df.to_csv(GOLD_PATH, index=False)
        print(f"Dados processados com sucesso para {GOLD_PATH}")
        return True
    except Exception as e:
        print(f"Erro ao processar dados para Gold: {str(e)}")
        raise e

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