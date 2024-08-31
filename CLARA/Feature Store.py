# Databricks notebook source
table = "fs_transacoesC"
with open(f"{table}.sql", "r") as open_file:
    query = open_file.read()

# COMMAND ----------

datas = ['2024-02-01',
        '2024-03-01',
        '2024-04-01',
        '2024-05-01',
        '2024-06-01',
        '2024-07-01',
        '2024-08-01']
for data in datas:
    #DEFINICAO DA QUERY
    query_format = query.format(data = data)

    #EXECUCAO
    df = spark.sql(query_format)

    #DELECAO DA DATA DE SAFRA QUE VAI SER INGERIDA
    try:
        spark.sql(f"DELETE FROM sandbox.med.{table} WHERE dtRef = '{data}'")
    except:
        print("Tabela ainda nao existe. Criando...")

    #INGESTAO/SALVAR
    (df.write
    .mode("append")
    .format("delta")
    .saveAsTable(f"sandbox.med.{table}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM sandbox.med.fs_transacoesC
# MAGIC
