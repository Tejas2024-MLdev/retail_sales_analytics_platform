# Databricks notebook source
silver_df = spark.table("silver_orders")

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql.functions import spark_partition_id
silver_df.select(spark_partition_id()).distinct().count()

# COMMAND ----------

silver_df.explain(True)

# COMMAND ----------

silver_df.cache()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

