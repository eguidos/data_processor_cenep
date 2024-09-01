from pyspark.sql import SparkSession

def load_data(spark, file_path):
    """
    Carrega um arquivo CSV do caminho especificado utilizando PySpark.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        file_path (str): Caminho para o arquivo CSV.

    Returns:
        DataFrame: DataFrame carregado a partir do CSV.
    """
    df = spark.read.options(header=True, delimiter=";", encoding="latin1").csv(file_path)
    return df

def write_data_to_s3(df, s3_path, file_format="parquet", mode="overwrite"):
    """
    Escreve um DataFrame no caminho S3 especificado utilizando PySpark.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        s3_path (str): Caminho no S3 onde o DataFrame será salvo.
        file_format (str): Formato do arquivo a ser salvo (parquet, csv, etc.). Default é "parquet".
        mode (str): Modo de gravação (append, overwrite, etc.). Default é "overwrite".
    """
    df.write.format(file_format).mode(mode).save(s3_path)
