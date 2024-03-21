# Databricks notebook source

# Setting the configuration for Azure Blob Storage account access
spark.conf.set(
"fs.azure.account.key.azure1dl.dfs.core.windows.net",
"igpc2TN98hvahc2VXBe/EyUL3IsCmzEI65iD/jBJgI79Wnbf8udIlO0o005FehC4SLWdB5sPTYRv+AStgpIBCg==")

# Listing files in the Azure Blob Storage
display(dbutils.fs.ls("abfss://demo@azure1dl.dfs.core.windows.net"))

# Reading a CSV file from Azure Blob Storage into a Spark DataFrame (Bronze Level)
df = spark.read.option("header", True).option("infershema", True).csv("abfss://demo@azure1dl.dfs.core.windows.net")

# Displaying the content of the DataFrame
df.display()

# Renaming columns in the DataFrame for clarity (Silver Level, Transformations)
df2 = df.withColumnRenamed("circuitid", "circuit_Id")\
    .withColumnRenamed("circuitRef", "circuit_ref")

# Selecting specific columns from the DataFrame
from pyspark.sql.functions import *
df3 = df2.select(col("circuit_Id"), col("circuit_ref"), col("name"), col('location'), col('country'))

# Writing the modified DataFrame to a new location in Azure Blob Storage
df3.write.mode("overwrite").parquet("abfss://demo2@azure1dl.dfs.core.windows.net")

# Reading the Parquet file from Azure Blob Storage into a Spark DataFrame (Gold Level)
Azure_Gen2 = spark.read.parquet("abfss://demo2@azure1dl.dfs.core.windows.net")

# Displaying the content of the DataFrame
Azure_Gen2.display()
