# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE lista_filmes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS lista_filmes
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/temp/api_tmdb/lista_filmes_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE detalhe_filme

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS detalhe_filme
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/user/hive/warehouse/detalhe_filme_final_14_08"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS oscar
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/FileStore/oscar_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS d_filmes
# MAGIC USING JSON
# MAGIC LOCATION "dbfs:/user/hive/warehouse/results.json"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS d_genero
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/user/hive/warehouse/genero_delta_table"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM genero LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS d_creditos
# MAGIC USING JSON
# MAGIC LOCATION "dbfs:/user/hive/warehouse/crew_cast.json"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM d_creditos