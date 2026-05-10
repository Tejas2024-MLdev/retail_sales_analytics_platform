# Databricks notebook source
bronze_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data/bronze/orders"
)

# COMMAND ----------

bronze_df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

bronze_df.filter(
    col("Customer_ID").isNull()
).count()

# COMMAND ----------

silver_df = bronze_df.filter(
    col("Customer_ID").isNotNull()
)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.filter(
    col("Quantity") > 0
)

# COMMAND ----------

silver_df = silver_df.filter(
    col("Price") > 0
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "SalesAmount",
    col("Quantity") * col("Price")
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "InvoiceDate",
    to_timestamp("InvoiceDate")
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

print("Bronze Count:", bronze_df.count())
print("Silver Count:", silver_df.count())

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_orders")
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/workspace/default/data/silver/orders"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_orders LIMIT 10