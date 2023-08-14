# Databricks notebook source
import requests 
import json
from pyspark.sql.functions import *
from multiprocessing import Pool
from tqdm import tqdm

# COMMAND ----------

# Dicionário para armazenar as respostas
def get_data_d_filmes(lista_ids): 
    # Autenticação
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTk0YjJhZmNiYmE0ZjJmNzRiMmE2ODBmYTg2OTIxZiIsInN1YiI6IjY0YzQ1ZDIwOTVjZTI0MDBhZjFhYTRmNSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.3bFkf5W2CB13xZa5muW964PHtqLOyZHPHKQyUAlS-IE"
    }
    
    for id_filme in tqdm(lista_ids):
        # print(lista_ids)
        # print(id_filme)
        url = f"https://api.themoviedb.org/3/movie/{id_filme}"

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            resposta = response.json()
            api_dumps = json.dumps(resposta)
            # selecionando os atributos relevantes
            df_filme = spark.read.json(spark.sparkContext.parallelize([api_dumps]))
            # display(df_filme)
            df_filme.write.json(f'dbfs:/user/hive/warehouse/filmes_detalhes/{id_filme}.json')
        else:
            print(f"Erro na chamada GET para {url}. Código de status: {response.status_code} para o id = {id_filme}")

# COMMAND ----------

# Coleta de todos os filmes
lista_id_filme = spark.sql(""" SELECT DISTINCT _c0 AS id_filme FROM lista_filmes WHERE nvl(_c0, "id") != "id" """).collect()
to_download = [(row.id_filme,) for row in lista_id_filme]
# print(to_download)
with Pool(4) as pool:
    pool.map(get_data_d_filmes, to_download)