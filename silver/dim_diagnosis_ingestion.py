# Databricks notebook source
from pyspark.sql.functions import sha2, col, current_timestamp, monotonically_increasing_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcare.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.bronze.diagnosis_raw;

# COMMAND ----------

bronze_table = 'healthcare.bronze.diagnosis_raw'
silver_table = 'healthcare.silver.dim_diagnosis'
checkpoint_path = "abfss://healthcare@clutchstrg.dfs.core.windows.net/silver/dim_diagnosis/checkpoint/"


df_diagnosis_bronze = (
    spark.readStream.table(bronze_table)
)

df_patient_clean = (
    df_diagnosis_bronze
        .dropDuplicates(["diagnosis_code"])
        .withColumn("load_timestamp", current_timestamp())
)


from delta.tables import DeltaTable

def merge_dim_diagnosis(batch_df, batch_id):
    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    # Load Delta table by name and upsert
    dim_diagnosis = DeltaTable.forName(spark, silver_table)

    (dim_diagnosis.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.diagnosis_code = s.diagnosis_code"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())



(
    df_patient_clean.writeStream
        .foreachBatch(merge_dim_diagnosis)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.silver.dim_diagnosis;