# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcare.silver;

# COMMAND ----------

bronze_table = 'healthcare.bronze.hospital_raw'
silver_table = 'healthcare.silver.dim_hospital'
checkpoint_path = "abfss://healthcare@clutchstrg.dfs.core.windows.net/silver/dim_hospital/checkpoint/"

# COMMAND ----------

from pyspark.sql.functions import sha2, col, current_timestamp, monotonically_increasing_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.bronze.hospital_raw;

# COMMAND ----------

df_hospital_bronze = (
    spark.readStream.table(bronze_table)
)

df_patient_clean = (
    df_hospital_bronze
        .dropDuplicates(["hospital_id"])
        .withColumn("load_timestamp", current_timestamp())
)



# COMMAND ----------

from delta.tables import DeltaTable

def merge_dim_hospital(batch_df, batch_id):
    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    # Load Delta table by name and upsert
    dim_hospital = DeltaTable.forName(spark, silver_table)

    (dim_hospital.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.hospital_id = s.hospital_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

(
    df_patient_clean.writeStream
        .foreachBatch(merge_dim_hospital)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.silver.dim_hospital;