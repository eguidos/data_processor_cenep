from pyspark.sql import functions as f
def calculate_metrics(df):
    # 1. Valor Total das Multas por Ano
    df_valor_multa = df.groupBy(f.year("DATA_INICIO_SANCAO").alias("ano"), "CATEGORIA_DA_SANCAO", "ORGAO_SANCIONADOR") \
                    .agg(f.sum("VALOR_DA_MULTA").alias("valor_total_multa"))

    
    # 2. Número de Sanções por Categoria e Ano
    df_num_sancoes_categoria = df.groupBy(f.year("DATA_INICIO_SANCAO").alias("ano"), f.col("CATEGORIA_DA_SANCAO").alias("SANCAO")) \
                                 .agg(f.count("*").alias("numero_sancoes"))
    
    # 3. Tempo Médio de Duração das Sanções por Ano e Categoria
    df_tempo_medio_duracao = df.withColumn("duracao_sancao", 
                                           f.datediff(f.col("DATA_FINAL_SANCAO"), f.col("DATA_INICIO_SANCAO"))
                                          ).groupBy(f.year("DATA_INICIO_SANCAO").alias("ano"), "CATEGORIA_DA_SANCAO") \
                                           .agg(f.avg("duracao_sancao").alias("tempo_medio_duracao_sancao"))
    
    # 4. Número de Sanções por Órgão Sancionador e Ano
    df_num_sancoes_orgao = df.groupBy(f.year("DATA_INICIO_SANCAO").alias("ano"), f.col("ORGAO_SANCIONADOR").alias("org_sancionador")) \
                             .agg(f.count("*").alias("numero_sancoes_orgao"))
    
    # 5. Total de Empresas Sancionadas por Ano
    df_num_empresas_sancionadas = df.groupBy(f.year("DATA_INICIO_SANCAO").alias("ano")) \
                                    .agg(f.countDistinct("CPF_OU_CNPJ_DO_SANCIONADO").alias("numero_empresas_sancionadas"))
    
    # Comece unindo df_valor_multa e df_num_empresas_sancionadas (ambos têm apenas "ano")
    df_metrics = df_valor_multa.join(df_num_empresas_sancionadas, "ano", "outer")
    
    # Junte com df_num_sancoes_categoria e df_tempo_medio_duracao, que compartilham "ano" e "CATEGORIA_DA_SANCAO"
    df_metrics = (df_metrics.join(df_num_sancoes_categoria, ["ano"], "outer") 
                           .join(df_tempo_medio_duracao.alias("medio"), ["ano", "CATEGORIA_DA_SANCAO"], "outer") 
                            .drop(f.col("medio.CATEGORIA_DA_SANCAO")))
    
    # Junte com df_num_sancoes_orgao usando "ano" e "ORGAO_SANCIONADOR"
    df_metrics = df_metrics.join(df_num_sancoes_orgao, ["ano"], "outer").distinct()
    
    return df_metrics