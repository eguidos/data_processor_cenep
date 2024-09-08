import os
from dotenv import load_dotenv
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Funções para recuperar as variáveis de ambiente específicas
def get_raw_data_path():
    return os.getenv("RAW_DATA_PATH")

def get_staged_data_path():
    return os.getenv("STAGED_DATA_PATH")

def get_trusted_data_path():
    return os.getenv("TRUSTED_DATA_PATH")