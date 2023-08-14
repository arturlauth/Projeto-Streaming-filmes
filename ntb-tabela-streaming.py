# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data = [('8','Netflix',55.90), ('384','HBO Max',34.90), ('119','Amazon Prime Video', 14.90), ('337','Disney Plus', 33.90)]
columns = ["id_streaming","nome_streaming","valor_streaming"]
df = spark.createDataFrame(data=data, schema = columns)
display(df)

# COMMAND ----------

df.write.json(f'dbfs:/user/hive/warehouse/tabela_streaming_2.json')