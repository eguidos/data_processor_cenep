import boto3
from pyspark.sql import SparkSession
from pyspark import SparkConf

def get_credentials(profile_name):
    # Obtém as credenciais do perfil AWS especificado usando boto3
    session = boto3.Session(profile_name=profile_name)
    credentials = session.get_credentials().get_frozen_credentials()
    return credentials

def init_spark_session(app_name="CNEP_Metrics"):
    credentials = get_credentials("dataeanalytics")
    conf = (
        SparkConf()
        .setAppName(app_name)
        .set("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.hadoop.fs.s3a.access.key", credentials.access_key)
        .set("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
        #.set("spark.hadoop.fs.s3a.session.token", credentials.token)  # Para credenciais temporárias
        .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .set("spark.sql.shuffle.partitions", "4")
        .setMaster("local[*]")
    )
    
    
    # Cria a sessão Spark usando as configurações definidas
    return SparkSession.builder.config(conf=conf).getOrCreate()
