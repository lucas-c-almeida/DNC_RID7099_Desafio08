import unittest
import os
import pandas as pd
from pathlib import Path
import shutil
from unittest.mock import patch

# Importa módulos de processamento de dados
from app.data_processing import (
    upload_raw_data_to_bronze,
    process_bronze_to_silver, 
    process_silver_to_gold
)


class TestDataProcessingPipeline(unittest.TestCase):
    """Testes para o pipeline de processamento de dados."""
    
    def setUp(self):
        """Configuração inicial antes dos testes."""
        # Define o diretório base para os testes
        self.base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self.test_data_dir = self.base_dir / 'test_data'
        
        # Cria diretórios para os testes
        os.makedirs(self.test_data_dir / '0_raw', exist_ok=True)
        os.makedirs(self.test_data_dir / '1_bronze', exist_ok=True)
        os.makedirs(self.test_data_dir / '2_silver', exist_ok=True)
        os.makedirs(self.test_data_dir / '3_gold', exist_ok=True)
        
        # Sobrescreve os caminhos originais para os caminhos de teste
        self.patcher = patch('app.data_paths.BASE_DIR', self.test_data_dir)
        self.mock_base_dir = self.patcher.start()
        
        # Atualiza as variáveis de caminho diretamente no módulo data_paths
        import app.data_paths as paths
        paths.RAW_DATA_PATH = self.test_data_dir / '0_raw/raw_data.csv'
        paths.BRONZE_PATH = self.test_data_dir / '1_bronze/bronze.csv'
        paths.SILVER_PATH = self.test_data_dir / '2_silver/silver.csv'
        paths.GOLD_PATH = self.test_data_dir / '3_gold/gold.csv'
        
        # Chama setup_paths para garantir que todos os diretórios necessários existam
        paths.setup_paths()
        
        # Cria um arquivo raw_data.csv de teste
        self.create_test_raw_data()
    
    def create_test_raw_data(self):
        """Cria um arquivo CSV de teste com dados simulados."""
        data = {
            'name': ['John Doe', 'Jane Smith', 'Invalid User', '', 'Mary Johnson'],
            'email': ['john.doe@example.com', 'jane.smith@example.com', 'invalid-email', 'missing@email.com', 'mary.johnson@example.com'],
            'date_of_birth': ['1990-01-01', '1985-05-15', '', '2000-10-10', '1975-03-20'],
            'signup_date': ['2023-01-15', '2023-02-20', '2023-03-25', '2023-04-30', '2023-05-05'],
            'status': ['active', 'inactive', 'active', 'inactive', 'active']
        }
        df = pd.DataFrame(data)
        import app.data_paths as paths
        df.to_csv(paths.RAW_DATA_PATH, index=False)
    
    def tearDown(self):
        """Limpeza após os testes."""
        # Finaliza o patcher
        self.patcher.stop()
        
        # Remove os diretórios de teste
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)
    
    def test_pipeline_from_raw_to_gold(self):
        """
        Testa se a função upload_raw_data_to_bronze está lendo o arquivo raw_data.csv 
        e se todo o pipeline funciona corretamente até criar o arquivo gold.csv
        """
        import app.data_paths as paths
        
        # 1. Verifica se o arquivo raw_data.csv foi criado corretamente
        self.assertTrue(os.path.exists(paths.RAW_DATA_PATH), "O arquivo raw_data.csv não foi criado")
        
        # 2. Executa a função upload_raw_data_to_bronze
        result_bronze = upload_raw_data_to_bronze()
        self.assertTrue(result_bronze, "A função upload_raw_data_to_bronze falhou")
        
        # 3. Verifica se o arquivo bronze.csv foi criado
        self.assertTrue(os.path.exists(paths.BRONZE_PATH), "O arquivo bronze.csv não foi criado")
        
        # 4. Verifica o conteúdo do arquivo bronze.csv
        bronze_df = pd.read_csv(paths.BRONZE_PATH)
        self.assertEqual(len(bronze_df), 5, "O número de registros em bronze.csv está incorreto")
        
        # 5. Executa a função process_bronze_to_silver
        result_silver = process_bronze_to_silver()
        self.assertTrue(result_silver, "A função process_bronze_to_silver falhou")
        
        # 6. Verifica se o arquivo silver.csv foi criado
        self.assertTrue(os.path.exists(paths.SILVER_PATH), "O arquivo silver.csv não foi criado")
        
        # 7. Executa a função process_silver_to_gold
        result_gold = process_silver_to_gold()
        self.assertTrue(result_gold, "A função process_silver_to_gold falhou")
        
        # 8. Verifica se o arquivo gold.csv foi criado
        self.assertTrue(os.path.exists(paths.GOLD_PATH), "O arquivo gold.csv não foi criado")
        
        # 9. Verifica o conteúdo do arquivo gold.csv
        gold_df = pd.read_csv(paths.GOLD_PATH)
        self.assertGreater(len(gold_df), 0, "O arquivo gold.csv está vazio")
        
        # 10. Verifica se as colunas esperadas estão presentes no arquivo gold.csv
        expected_columns = {'age_group', 'active', 'inactive', 'total'}
        self.assertTrue(expected_columns.issubset(gold_df.columns), f"O arquivo gold.csv não contém todas as colunas esperadas: {expected_columns}")


if __name__ == "__main__":
    unittest.main()