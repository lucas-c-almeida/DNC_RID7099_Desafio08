import pandas as pd
from datetime import datetime

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

def main():
    print("Iniciando processamento de Silver para Gold...")
    try:
        # Lê o arquivo CSV da camada Silver
        df = pd.read_csv('2_silver/silver.csv')
        
        # Certifica-se de que temos a coluna idade
        if 'age' not in df.columns:
            df['age'] = df['date_of_birth'].apply(lambda x: calculate_age(pd.to_datetime(x, errors='coerce')))
        
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
        pivot_df.to_csv('3_gold/gold.csv', index=False)
        print("Dados processados com sucesso para Gold")
        
    except Exception as e:
        print(f"Erro ao processar dados para Gold: {str(e)}")
        raise e

if __name__ == "__main__":
    main()