# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcare.silver;

# COMMAND ----------

bronze_table = 'healthcare.bronze.visit_raw'
silver_table = 'healthcare.silver.fact_visit'
checkpoint_path = "abfss://healthcare@clutchstrg.dfs.core.windows.net/silver/fact_visit/checkpoint/"

# COMMAND ----------


from pyspark.sql.functions import col, lag, to_date, datediff, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.bronze.visit_raw;

# COMMAND ----------

silver_patient_table = "healthcare.silver.dim_patient"
silver_hospital_table = "healthcare.silver.dim_hospital"
silver_diagnosis_table = "healthcare.silver.dim_diagnosis"
bronze_table = "healthcare.bronze.visit_raw"

# COMMAND ----------


df_patient = spark.read.table(silver_patient_table)
df_hospital = spark.read.table(silver_hospital_table)
df_diagnosis = spark.read.table(silver_diagnosis_table)

# COMMAND ----------

df_visit_bronze = (
    spark.readStream.table(bronze_table)
)

# COMMAND ----------

# Rename columns to avoid duplicates
df_patient = df_patient.withColumnRenamed("city", "patient_city")

df_hospital = df_hospital.withColumnRenamed("city", "hospital_city")


# Join clean fact visit with dimension tables
df_fact_combined = (
    df_visit_bronze
        .join(df_patient, "patient_id", "left")
        .join(df_hospital, "hospital_id", "left")
        .join(df_diagnosis, "diagnosis_code", "left")
        .withColumn("admission_date", to_date("admission_date"))
        .withColumn("discharge_date", to_date("discharge_date"))
        .withColumn("load_timestamp", current_timestamp())
)

# COMMAND ----------

# -------------------------
# Merge into Silver fact_visit
# -------------------------
def merge_fact_visit(batch_df, batch_id):

    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    fact = DeltaTable.forName(spark, silver_table)
    fact.alias("t").merge(
        batch_df.alias("s"),
        "t.visit_id = s.visit_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

# -------------------------
# Run as availableNow incremental
# -------------------------
(
    df_fact_combined.drop("load_timestamp").writeStream
        .foreachBatch(merge_fact_visit)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.silver.fact_visit;