# Databricks notebook source
# MAGIC %sh
# MAGIC pip install tqdm

# COMMAND ----------

import requests 
import json
from pyspark.sql.functions import *
from tqdm import tqdm

# COMMAND ----------

# Dicionário para armazenar as respostas
responses = []
lista_id_filme = spark.sql(""" SELECT DISTINCT _c0 AS id_filme FROM lista_filmes WHERE nvl(_c0, "id") != "id" """).collect()

# Autenticação

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTk0YjJhZmNiYmE0ZjJmNzRiMmE2ODBmYTg2OTIxZiIsInN1YiI6IjY0YzQ1ZDIwOTVjZTI0MDBhZjFhYTRmNSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.3bFkf5W2CB13xZa5muW964PHtqLOyZHPHKQyUAlS-IE"
}
for row in tqdm(lista_id_filme):
    url = f"https://api.themoviedb.org/3/movie/{row.id_filme}/credits?language=en-US"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        responses.append(response.json())
        # selecionando os atributos relevantes

    else:
        print(f"Erro na chamada GET para {url}. Código de status: {response.status_code} para o id = {row.id_filme}")
api_dumps = json.dumps(responses)
# selecionando os atributos relevantes

df_crew_cast = spark.read.json(spark.sparkContext.parallelize([api_dumps]))
display(df_crew_cast)

# COMMAND ----------

df_crew_cast.write.json(f'dbfs:/user/hive/warehouse/crew_cast.json')