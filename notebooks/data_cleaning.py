# Databricks notebook source
silver_df = spark.table("silver_orders")

# COMMAND ----------

silver_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

null_customer_df = silver_df.filter(
    col("Customer_ID").isNull()
)

# COMMAND ----------

null_customer_df.count()

# COMMAND ----------

negative_sales_df = silver_df.filter(
    col("SalesAmount") < 0
)

# COMMAND ----------

invalid_price_df = silver_df.filter(
    col("Price") <= 0
)

# COMMAND ----------

duplicate_invoice_df = (
    silver_df.groupBy("Invoice")
    .count()
    .filter(col("count") > 1)
)

# COMMAND ----------

bad_records_df = silver_df.filter(
    (col("Customer_ID").isNull()) |
    (col("SalesAmount") < 0) |
    (col("Price") <= 0)
)

# COMMAND ----------

bad_records_df.show()

# COMMAND ----------

(
    bad_records_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bad_records")
)

# COMMAND ----------

total_records = silver_df.count()

null_customer_count = null_customer_df.count()

negative_sales_count = negative_sales_df.count()

invalid_price_count = invalid_price_df.count()

# COMMAND ----------

print("Total Records:", total_records)

print("Null Customer IDs:", null_customer_count)

print("Negative Sales Records:", negative_sales_count)

print("Invalid Price Records:", invalid_price_count)

# COMMAND ----------

dq_summary = [
    ("Total Records", total_records),
    ("Null Customers", null_customer_count),
    ("Negative Sales", negative_sales_count),
    ("Invalid Prices", invalid_price_count)
]

# COMMAND ----------

dq_summary_df = spark.createDataFrame(
    dq_summary,
    ["Metric", "Count"]
)

# COMMAND ----------

(
    dq_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dq_metrics")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dq_metrics

# COMMAND ----------

