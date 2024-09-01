import sys, os
# Adicione o diretório principal do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analytics import metrics
from transform import correct_data_types, clean_data
from config import init_spark_session, load_data
from pyspark.sql import functions as f

def main():
    
    # Inicializar a sessão Spark
    spark = init_spark_session()
    
    # Carregar e limpar os dados
    ceaf = load_data(spark, "s3a://dataeanalytics-data-lake-raw/cenep/20240711_CNEP.csv")
    ceaf = clean_data(ceaf)
    
    # Corrigir os tipos de dados
    ceaf_cleaned = correct_data_types(ceaf)
    
    # Calcular as métricas
    df_metrics = metrics.calculate_metrics(ceaf_cleaned)
    df_metrics.show()
    
if __name__ == "__main__":
    main()