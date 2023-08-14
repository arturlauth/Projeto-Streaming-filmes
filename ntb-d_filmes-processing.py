# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE d_filmes

# COMMAND ----------

# df_oscar = spark.sql(""" SELECT * FROM oscar WHERE nvl(_c6, "False") NOT IN ("False", "winner") """)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS d_filmes
# MAGIC USING JSON
# MAGIC LOCATION "dbfs:/user/hive/warehouse/results.json"

# COMMAND ----------

from pyspark.sql.functions import col, when, lit
from pyspark.sql.functions import explode, size

# COMMAND ----------

df_filmes = spark.table("d_filmes")

# COMMAND ----------

df_filmes.printSchema()

# COMMAND ----------

df_filmes = df_filmes.dropDuplicates(["id", "original_title"])
display(df_filmes.count())

# COMMAND ----------

display(df_filmes)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), count(id), count(imdb_id), count(genres), count(original_title), count(budget), count(release_date)
# MAGIC FROM d_filmes

# COMMAND ----------

df_filmes = df_filmes.where(col("imdb_id").isNotNull())

# COMMAND ----------

display(df_filmes.count())

# COMMAND ----------

genero_exploded = (df_filmes.select("id", "genres")
    .withColumn("genero", explode("genres"))
)

display(genero_exploded.where(size("genres") > 0))

# COMMAND ----------

display(genero_exploded.groupBy("id").count())

# COMMAND ----------

df_generos = genero_exploded.withColumn("nome_do_genero", col("genero.name"))
df_generos = df_generos.select(col("id").alias("id_filme"), "nome_do_genero")
display(df_generos)

# COMMAND ----------

df_filmes_renomeado = df_filmes.withColumnRenamed("budget", "orcamento").withColumnRenamed("id", "id_filme").withColumnRenamed("original_title", "nome_filme").withColumnRenamed("overview", "sinopse").withColumnRenamed("popularity", "popularidade").withColumnRenamed("release_date", "data_lancamento").withColumnRenamed("runtime", "duracao").withColumnRenamed("vote_average", "nota").withColumnRenamed("revenue", "receita").drop("genres")

# COMMAND ----------

display(df_filmes_renomeado.count())

# COMMAND ----------

df_estudio = df_filmes_renomeado.select(explode("production_companies").alias("estudio"), "id_filme")
df_nome_estudio = df_estudio.withColumn("estudio", col("estudio.name"))
df_nome_estudio.createOrReplaceTempView("estudio_temp")
df_nome_estudio = spark.sql(""" WITH added_row_number AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id_filme ORDER BY id_filme DESC) AS row_number
  FROM estudio_temp
)
SELECT
  *
FROM added_row_number
WHERE row_number = 1; """)

df_filmes_com_estudio = df_filmes_renomeado.join(df_nome_estudio.select("id_filme", "estudio"), on="id_filme", how="left").drop("production_companies")
display(df_filmes_com_estudio)

# COMMAND ----------

df_creditos = spark.table("d_creditos")
display(df_creditos)

# COMMAND ----------

# extraindo o protagonista, primeiro da lista da api creditos
df_creditos_cast = df_creditos.select(explode("cast").alias("cast"), col("id").alias("id_filme") ) 
df_creditos_cast = df_creditos_cast.filter(col("cast.order") == 0)
df_creditos_cast = df_creditos_cast.withColumn("protagonista", col("cast.name"))

# df_filme = df_filme.withColumn("protagonista", lit(nome_protagonista))
df_filmes_com_protagonista = df_filmes_com_estudio.join(df_creditos_cast.select("protagonista", "id_filme"), on="id_filme", how="left")
display(df_filmes_com_protagonista)

# COMMAND ----------

# extraindo o diretor, primeiro da lista da api
df_creditos_crew = df_creditos.select(explode("crew").alias("crew"), col("id").alias("id_filme"))
df_diretor = df_creditos_crew.filter((col("crew.department") == "Directing") & (col("crew.job") == "Director"))
df_diretor = df_diretor.withColumn("diretor", col("crew.name")).withColumn("department", col("crew.department")).withColumn("job", col("crew.job"))
df_diretor.createOrReplaceTempView("crew_temp")
df_diretor = spark.sql(""" WITH added_row_number AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id_filme ORDER BY id_filme DESC) AS row_number
  FROM crew_temp
)
SELECT
  *
FROM added_row_number
WHERE row_number = 1; """)
df_filmes_com_diretor = df_filmes_com_protagonista.join(df_diretor.select("diretor", "id_filme"), on="id_filme", how="left")
display(df_filmes_com_diretor)

# COMMAND ----------

display(df_filmes_com_diretor.count())

# COMMAND ----------


# carregando tabela com informações do oscar
df_ganhou_oscar = spark.sql(""" SELECT * FROM oscar WHERE nvl(_c6, "False") NOT IN ("False", "winner") """)
df_ganhou_oscar = df_ganhou_oscar.filter(col("_c5") != "null")
# coletando quantidade de oscar
nome_filme = df_filmes_com_diretor.collect()[0]["nome_filme"]
qtd_oscar = df_ganhou_oscar.select(col("_c5").alias("nome_filme"), "_c6").groupby("nome_filme").count()
df_filmes_com_oscar = df_filmes_com_diretor.join(qtd_oscar.select(col("count").alias("qtd_oscar"), "nome_filme"), on="nome_filme", how="left")
display(df_filmes_com_oscar)

# COMMAND ----------

df_filmes_com_oscar.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("dbfs:/user/hive/warehouse/detalhe_filme_final_14_08")