# Databricks notebook source
df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/default/data/online_retail_II.csv")
)

# COMMAND ----------

df_raw.show()

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

bronze_path = "/Volumes/workspace/default/data/bronze/"
silver_path = "/Volumes/workspace/default/data/silver/"
gold_path = "/Volumes/workspace/default/data/gold/"

# COMMAND ----------

df_raw = df_raw.toDF(
    *[
        c.strip()
         .replace(" ", "_")
         .replace("/", "_")
         .replace("-", "_")
        for c in df_raw.columns
    ]
)

# COMMAND ----------

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/data/bronze/orders")
)

# COMMAND ----------

display(dbutils.fs.ls(bronze_path + "orders"))