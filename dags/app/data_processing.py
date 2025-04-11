"""
Funções de processamento de dados para as camadas Bronze, Silver e Gold.
"""
import pandas as pd
import re
from datetime import datetime
from app.data_paths import RAW_DATA_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH

# Funções auxiliares para processamento de dados
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
    """
    Processa e valida um endereço de email.
    """
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

# Funções principais do pipeline de dados
def upload_raw_data_to_bronze():
    """
    Função para carregar dados brutos para a camada Bronze.
    """
    import app.data_paths as paths  # Re-import to ensure updated paths are used

    print("Iniciando carregamento de dados brutos para Bronze...")
    try:
        # Lê o arquivo CSV 
        print(f"RAW_DATA_PATH: {paths.RAW_DATA_PATH}")
        df = pd.read_csv(paths.RAW_DATA_PATH)
        
        # Salva o dataframe na camada Bronze
        print(f"Tentando salvar em BRONZE_PATH: {paths.BRONZE_PATH}")
        df.to_csv(paths.BRONZE_PATH, index=False)
        print(f"Dados carregados com sucesso para {paths.BRONZE_PATH}")
        return True
    except Exception as e:
        print(f"Erro ao carregar dados para Bronze: {str(e)}")
        raise e

def process_bronze_to_silver():
    """
    Função para processar dados da Bronze para a camada Silver.
    """
    import app.data_paths as paths  # Re-import to ensure updated paths are used

    print("Iniciando processamento de Bronze para Silver...")
    try:
        # Lê o arquivo CSV da camada Bronze
        df = pd.read_csv(paths.BRONZE_PATH)
        
        ## Processamento de dados

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
        print(f"Tentando salvar em SILVER_PATH: {paths.SILVER_PATH}")
        df.to_csv(paths.SILVER_PATH, index=False)
        print(f"Dados processados com sucesso para {paths.SILVER_PATH}")
        return True
    except Exception as e:
        print(f"Erro ao processar dados para Silver: {str(e)}")
        raise e

def process_silver_to_gold():
    """
    Função para processar dados da Silver para a camada Gold.
    """
    import app.data_paths as paths  # Re-import to ensure updated paths are used

    print("Iniciando processamento de Silver para Gold...")
    try:
        # Lê o arquivo CSV da camada Silver
        df = pd.read_csv(paths.SILVER_PATH)
        
        # Definir faixas etárias
        age_bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
        age_labels = ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90', '91-100', '101+']
        
        # Categorizar idades em faixas etárias
        df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels, right=False)
        
        # Agregação por faixa etária e subscription_status (active ou inactive)
        agg_df = df.groupby(['age_group', 'subscription_status']).size().reset_index(name='count')
        
        # Pivotear a tabela para ter faixas etárias nas linhas e subscription_status nas colunas
        pivot_df = agg_df.pivot_table(index='age_group', columns='subscription_status', values='count', fill_value=0).reset_index()
        
        # Garantir que todas as colunas de subscription_status existem
        if 'active' not in pivot_df.columns:
            pivot_df['active'] = 0
        if 'inactive' not in pivot_df.columns:
            pivot_df['inactive'] = 0
        
        # Calcular o total de usuários por faixa etária
        pivot_df['total'] = pivot_df['active'] + pivot_df['inactive']
        
        # Salva o dataframe agregado na camada Gold
        print(f"Tentando salvar em GOLD_PATH: {paths.GOLD_PATH}")
        pivot_df.to_csv(paths.GOLD_PATH, index=False)
        print(f"Dados processados com sucesso para {paths.GOLD_PATH}")
        return True
    except Exception as e:
        print(f"Erro ao processar dados para Gold: {str(e)}")
        raise e