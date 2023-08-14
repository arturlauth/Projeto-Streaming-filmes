# Databricks notebook source
import requests 
import json
from pyspark.sql.functions import *

# COMMAND ----------

url = "https://api.themoviedb.org/3/discover/movie?watch_region=BR&with_watch_providers=8"

# url = "https://api.themoviedb.org/3/discover/movie?region=BR&with_watch_providers=2&without_watch_providers=8%2C384%2C119%2C337"


headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTk0YjJhZmNiYmE0ZjJmNzRiMmE2ODBmYTg2OTIxZiIsInN1YiI6IjY0YzQ1ZDIwOTVjZTI0MDBhZjFhYTRmNSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.3bFkf5W2CB13xZa5muW964PHtqLOyZHPHKQyUAlS-IE"
}

response = requests.get(url, headers=headers).json()

# COMMAND ----------

api_dumps = json.dumps(response)
api_dumps

# COMMAND ----------

#dbutils.fs.put('/temp/watchmode/345534.json', api_dumps, True)
dbutils.fs.put("/temp/api_tmdb/list.json", api_dumps, True)

# COMMAND ----------

dbutils.fs.ls('/temp/api_tmdb')

# COMMAND ----------

df = spark.read.json('/temp/api_tmdb/list.json')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#Explodir a coluna "results" para transformar cada elemento da lista em uma linha separada
df_expanded = df.select("page", "total_pages", "total_results", explode("results").alias("result"))

#Selecionar as informações relevantes de "results"
df_resultados_separados = df_expanded.select("page", "result.*")

df_resultados_separados.show()

# COMMAND ----------

#Registrar o DataFrame como uma tabela temporária
df1 = df_resultados_separados.select("id")
df1.show()

# COMMAND ----------

# Obtenha o valor da coluna "total_pages" usando collect() e first()
total_pages_value = df_expanded.select("total_pages").collect()[0]["total_pages"]

# Converta o valor para inteiro
total_pages = int(total_pages_value)

total_pages

# COMMAND ----------

#coletando todos os filmes para os streamings: netflix, apple tv(nao deu certo, api só deixa 500 paginas), hbo max, amazon prime video, disney plus

lista_streamings = ["8", "384", "119", "337"] #netflix, hbo max, amazon prime video, disney plus

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTk0YjJhZmNiYmE0ZjJmNzRiMmE2ODBmYTg2OTIxZiIsInN1YiI6IjY0YzQ1ZDIwOTVjZTI0MDBhZjFhYTRmNSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.3bFkf5W2CB13xZa5muW964PHtqLOyZHPHKQyUAlS-IE"
}

columns = ["id", "original_title", "streaming"]
data = [(0, "", "")]
df1 = spark.createDataFrame(data, schema=columns)

for streaming in lista_streamings:    
    
    regiao = "BR"

    url = f"https://api.themoviedb.org/3/discover/movie?watch_region={regiao}&with_watch_providers={streaming}"

    # pegando pagina de resultado 
    response = requests.get(url, headers=headers).json()
    api_dumps = json.dumps(response)
    df = spark.read.json(spark.sparkContext.parallelize([api_dumps]))
    
    # Obtenha o valor da coluna "total_pages" usando collect() e first()
    total_pages_value = df.select("total_pages").collect()[0]["total_pages"]

    # Converta o valor para inteiro
    total_pages = int(total_pages_value)

    for page in range(total_pages):

        url = f"https://api.themoviedb.org/3/discover/movie?page={page+1}&watch_region={regiao}&with_watch_providers={streaming}"
        
        # pegando pagina de resultado 
        response = requests.get(url, headers=headers).json()
        api_dumps = json.dumps(response)
        df = spark.read.json(spark.sparkContext.parallelize([api_dumps]))

        #Explodir a coluna "results" para transformar cada elemento da lista em uma linha separada
        df_expanded = df.select("page", "total_pages", "total_results", explode("results").alias("result"))

        # Selecionar as informações relevantes de "results" para a página atual
        df_resultados_id = df_expanded.select("result.id", "result.title")
        df_resultados_id = df_resultados_id.withColumn("streaming", lit(streaming))
        # Selecionar apenas a coluna "id" e fazer a união com o DataFrame df1
        df1 = df1.union(df_resultados_id)

# Exibir o DataFrame final com todos os IDs coletados
# df1.show()


# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.mode("overwrite").csv("/temp/api_tmdb/lista_filmes_2.csv", header=True)

# COMMAND ----------

dbutils.fs.ls('/temp/api_tmdb/')

# COMMAND ----------

df_filmes = spark.read.csv("/temp/api_tmdb/lista_filmes.csv")

# COMMAND ----------

df_filmes.filter(col("_c0") == "406563").show()

# COMMAND ----------

# Converta a tabela para o formato Delta
df_filmes.write.format("delta").mode("overwrite").save("dbfs:/temp/api_tmdb/lista_filmes_delta")

# COMMAND ----------

from delta.tables import DeltaTable

# Caminho para a tabela Delta
delta_table_path = "dbfs:/temp/api_tmdb/lista_filmes_delta"

# Carregue a tabela Delta
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Execute a otimização da tabela Delta
delta_table.optimize()

#df
df_filmes_delta = DeltaTable.forPath(spark, delta_table_path).toDF()
df_filmes_delta.show()

# COMMAND ----------

# validação dos dados
lista_streamings = ["8", "384", "119", "337"] #netflix, hbo max, amazon prime video, disney plus

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjYTk0YjJhZmNiYmE0ZjJmNzRiMmE2ODBmYTg2OTIxZiIsInN1YiI6IjY0YzQ1ZDIwOTVjZTI0MDBhZjFhYTRmNSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.3bFkf5W2CB13xZa5muW964PHtqLOyZHPHKQyUAlS-IE"
}

regiao = "BR"

for streaming in lista_streamings:
    # pegando pagina de resultado 
    url = f"https://api.themoviedb.org/3/discover/movie?watch_region={regiao}&with_watch_providers={streaming}"
    response = requests.get(url, headers=headers).json()
    api_dumps = json.dumps(response)
    df = spark.read.json(spark.sparkContext.parallelize([api_dumps]))

    # Obtenha o valor da coluna "total_pages" usando collect() e first()
    total_results = df.select("total_results").collect()[0]["total_results"]

    # Converta o valor para inteiro
    total_results_value = int(total_results)
    print(f'A api indica um total de :{total_results_value} filmes')
    num_filmes_streaming = df_filmes_delta.filter(col("_c2") == str(streaming)).count()
    print(f'A contagem do numero de linhas para o streaming: {num_filmes_streaming}')