import sys, os
# Adicione o diretório principal do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analytics import metrics
from transform import correct_data_types, clean_data
from config import init_spark_session, load_data, write_data_to_s3,  get_raw_data_path, get_staged_data_path, get_trusted_data_path
from pyspark.sql import functions as f
from datetime import datetime
# Gerar a data atual no formato 'YYYYMMDD'
current_date = datetime.now().strftime("%Y%m%d")

def main():
    
    # Inicializar a sessão Spark
    spark = init_spark_session()
    
    # Carregar e limpar os dados
    cnep = load_data(spark, f"{get_raw_data_path()}/cenep/20240711_CNEP.csv")
    cnep = clean_data(cnep)
    cnep = correct_data_types(cnep)
    
    #Salvar arquivo ccnorrigido na camada staged
    write_data_to_s3(cnep, f"{get_staged_data_path()}/cenep/inserted_at={current_date}/")
    # Calcular as métricas
    df_metrics = metrics.calculate_metrics(cnep)
    df_metrics.show()

    write_data_to_s3(df_metrics, f"{get_trusted_data_path()}/cenep/inserted_at={current_date}/")
    
if __name__ == "__main__":
    main()