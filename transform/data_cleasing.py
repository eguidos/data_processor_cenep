from pyspark.sql import functions as f
def clean_data(df):
        # Renomear colunas com caracteres corrompidos
    df = df.withColumnRenamed("Cï¿½DIGO DA SANï¿½ï¿½O", "CODIGO_DA_SANCAO") \
           .withColumnRenamed("NOME INFORMADO PELO ï¿½RGï¿½O SANCIONADOR", "NOME_INFORMADO_PELO_ORGAO_SANCIONADOR") \
           .withColumnRenamed("RAZï¿½O SOCIAL - CADASTRO RECEITA", "RAZAO_SOCIAL_CADASTRO_RECEITA") \
           .withColumnRenamed("Nï¿½MERO DO PROCESSO", "NUMERO_DO_PROCESSO") \
           .withColumnRenamed("CATEGORIA DA SANï¿½ï¿½O", "CATEGORIA_DA_SANCAO") \
           .withColumnRenamed("DATA INï¿½CIO SANï¿½ï¿½O", "DATA_INICIO_SANCAO") \
           .withColumnRenamed("DATA FINAL SANï¿½ï¿½O", "DATA_FINAL_SANCAO") \
           .withColumnRenamed("DATA PUBLICAï¿½ï¿½O", "DATA_PUBLICACAO") \
           .withColumnRenamed("PUBLICAï¿½ï¿½O", "PUBLICACAO") \
           .withColumnRenamed("DATA DO TRï¿½NSITO EM JULGADO", "DATA_DO_TRANSITO_EM_JULGADO") \
           .withColumnRenamed("ABRAGï¿½NCIA DEFINIDA EM DECISï¿½O JUDICIAL", "ABRAGENCIA_DEFINIDA_EM_DECISAO_JUDICIAL") \
           .withColumnRenamed("ï¿½RGï¿½O SANCIONADOR", "ORGAO_SANCIONADOR") \
           .withColumnRenamed("UF ï¿½RGï¿½O SANCIONADOR", "UF_ORGAO_SANCIONADOR") \
           .withColumnRenamed("ESFERA ï¿½RGï¿½O SANCIONADOR", "ESFERA_ORGAO_SANCIONADOR") \
           .withColumnRenamed("FUNDAMENTAï¿½ï¿½O LEGAL", "FUNDAMENTACAO_LEGAL")
    
    # Substituir espaços em branco por sublinhados nos nomes das colunas
    new_column_names = [col_name.replace(" ", "_") for col_name in df.columns]
    return df.toDF(*new_column_names)


def correct_data_types(df):
    # Remover vírgulas e converter a coluna VALOR_DA_MULTA para float
    df_cleaned = df.withColumn("VALOR_DA_MULTA", f.regexp_replace(f.col("VALOR_DA_MULTA"), ",", "."))
    df_cleaned = df_cleaned.withColumn("VALOR_DA_MULTA", f.col("VALOR_DA_MULTA").cast("float"))
    
    # Converter DATA_INICIO_SANCA e DATA_FIM_SANCAO de string para data
    df_cleaned = (df_cleaned.withColumn("DATA_INICIO_SANCAO", 
                                            f.to_date(f.col("DATA_INICIO_SANCAO"), "dd/MM/yyyy"))
                            .withColumn("DATA_FINAL_SANCAO",
                                        f.to_date(f.col("DATA_FINAL_SANCAO"), "dd/MM/yyyy")))
    
    # Converter a coluna "VALOR_DA_MULTA" para float
    df_cleaned = df_cleaned.withColumn("VALOR_DA_MULTA", f.col("VALOR_DA_MULTA").cast("float"))
    return df_cleaned

