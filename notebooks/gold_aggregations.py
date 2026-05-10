# Databricks notebook source
silver_df = spark.table("silver_orders")

# COMMAND ----------

silver_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

country_sales_df = (
    silver_df.groupBy("Country")
    .agg(
        round(sum("SalesAmount"), 2).alias("TotalRevenue")
    )
    .orderBy(col("TotalRevenue").desc())
)

# COMMAND ----------

country_sales_df.show()

# COMMAND ----------

top_products_df = (
    silver_df.groupBy("Description")
    .agg(
        round(sum("SalesAmount"), 2).alias("Revenue")
    )
    .orderBy(col("Revenue").desc())
)

# COMMAND ----------

top_products_df.show(10)

# COMMAND ----------

monthly_sales_df = (
    silver_df
    .withColumn("Year", year("InvoiceDate"))
    .withColumn("Month", month("InvoiceDate"))
    .groupBy("Year", "Month")
    .agg(
        round(sum("SalesAmount"), 2).alias("MonthlyRevenue")
    )
    .orderBy("Year", "Month")
)

# COMMAND ----------

monthly_sales_df.show()

# COMMAND ----------

customer_sales_df = (
    silver_df.groupBy("Customer_ID")
    .agg(
        round(sum("SalesAmount"), 2).alias("CustomerRevenue")
    )
    .orderBy(col("CustomerRevenue").desc())
)

# COMMAND ----------

customer_sales_df.show(5)

# COMMAND ----------

(
    country_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_country_sales")
)

# COMMAND ----------

(
    top_products_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_top_products")
)

# COMMAND ----------

(
    monthly_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_monthly_sales")
)

# COMMAND ----------

(
    customer_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_customer_sales")
)