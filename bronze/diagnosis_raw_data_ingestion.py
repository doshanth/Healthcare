# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcare.bronze;

# COMMAND ----------


source_path = "abfss://healthcare@clutchstrg.dfs.core.windows.net/staging/diagnosis/"
checkpoint_path = "abfss://healthcare@clutchstrg.dfs.core.windows.net/bronze/diagnosis_raw/checkpoint/"
schema_location = "abfss://healthcare@clutchstrg.dfs.core.windows.net/bronze/diagnosis_raw/schema/"

# Autoloader read
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("cloudFiles.maxFilesPerTrigger", 1) # READ ONE FILE AT A TIME
          .option("cloudFiles.schemaLocation", schema_location)  
          .load(source_path)
     )

# Write Bronze table (append)
(
    df.drop("_rescued_data")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .outputMode("append")
      .trigger(availableNow=True)   # AVAILABLE NOW → ingests all files & stops
      .toTable("healthcare.bronze.diagnosis_raw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.bronze.diagnosis_raw