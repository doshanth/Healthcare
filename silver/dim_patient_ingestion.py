# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcare.silver;

# COMMAND ----------

bronze_table = 'healthcare.bronze.patient_raw'
silver_table = 'healthcare.silver.dim_patient'
checkpoint_path = "abfss://healthcare@clutchstrg.dfs.core.windows.net/silver/dim_patient/checkpoint/"

# COMMAND ----------

from pyspark.sql.functions import sha2, col, current_timestamp, monotonically_increasing_id, concat_ws

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.bronze.patient_raw;

# COMMAND ----------

df_patient_bronze = (
    spark.readStream.table(bronze_table)
)

df_patient_clean = (
    df_patient_bronze
        .dropDuplicates(["patient_id"])
        .withColumn("patient_first_last_name_masked", sha2(concat_ws('|',col("first_name"),col("last_name")), 256))
        .withColumn("load_timestamp", current_timestamp())
)



# COMMAND ----------

from delta.tables import DeltaTable

def merge_dim_patient(batch_df, batch_id):
    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    # Load Delta table by name and upsert
    dim_patient = DeltaTable.forName(spark, silver_table)

    (dim_patient.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.patient_id = s.patient_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

(
    df_patient_clean.writeStream
        .foreachBatch(merge_dim_patient)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.silver.dim_patient;