# Databricks notebook source
df = spark.sql("SELECT * FROM hive_metastore.default.painel_rj")

for column in df.columns: #lista com os nomes das colunas
    df=df.withColumnRenamed(column,column.replace(" ","_"))
df.write.mode("overwrite").saveAsTable("painel_1") #sobrescreve

# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.default.painel_es")

for column in df.columns: #lista com os nomes das colunas
    df=df.withColumnRenamed(column,column.replace(" ","_"))
df.write.mode("overwrite").saveAsTable("painel_2") #sobrescreve

# COMMAND ----------

df = spark.sql("SELECT * FROM hive_metastore.default.painel_mt")

for column in df.columns: #lista com os nomes das colunas
    df=df.withColumnRenamed(column,column.replace(" ","_"))
df.write.mode("overwrite").saveAsTable("painel_3") #sobrescreve
