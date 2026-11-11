# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcare.silver.fact_visit;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.table("healthcare.silver.fact_visit")

# COMMAND ----------

w = (
    Window
        .partitionBy("patient_id")
        .orderBy("admission_date")
)

df_with_prev = (
    df.withColumn("previous_discharge", lag("discharge_date").over(w))
      .withColumn(
          "days_since_last_visit",
          datediff("admission_date", "previous_discharge")
      )
      .withColumn(
          "is_readmission_30d",
          when(col("days_since_last_visit") <= 30, 1).otherwise(0)
      )
)


# COMMAND ----------

display(df_with_prev)

# COMMAND ----------

gold_df = (
    df_with_prev.groupBy(
        "hospital_id",
        "hospital_name",
        "diagnosis_desc"
    )
    .agg(
        count("*").alias("total_visits"),
        sum("is_readmission_30d").alias("total_readmissions"),
        round(sum("is_readmission_30d") / count("*"), 3).alias("readmission_rate"),
        sum("cost").alias("total_cost"),
        avg("cost").alias("avg_cost")
    )
    .withColumn("gold_load_timestamp", current_timestamp())
)


# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_df.write.mode("overwrite").saveAsTable("healthcare.gold.hospital_disease_kpi")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which hospital has the highest readmission rate?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT hospital_name, readmission_rate
# MAGIC FROM healthcare.gold.hospital_disease_kpi
# MAGIC ORDER BY readmission_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which disease category causes maximum readmissions?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT diagnosis_desc, SUM(total_readmissions) AS readm
# MAGIC FROM healthcare.gold.hospital_disease_kpi
# MAGIC GROUP BY diagnosis_desc
# MAGIC ORDER BY readm DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ⭐ THIS GOLD TABLE ANSWERS THE CORE QUESTIONS YOU NEED FOR BI ⬇️
# MAGIC 1. Which hospital has the highest readmission rate?
# MAGIC SELECT hospital_name, readmission_rate
# MAGIC FROM anirvandecodes.gold.hospital_disease_kpi
# MAGIC ORDER BY readmission_rate DESC;
# MAGIC
# MAGIC 2. Which disease category causes maximum readmissions?
# MAGIC SELECT diagnosis_category, SUM(total_readmissions) AS readm
# MAGIC FROM anirvandecodes.gold.hospital_disease_kpi
# MAGIC GROUP BY diagnosis_category
# MAGIC ORDER BY readm DESC;
# MAGIC
# MAGIC 3. Which hospital is performing worst for cardiac patients?
# MAGIC SELECT *
# MAGIC FROM anirvandecodes.gold.hospital_disease_kpi
# MAGIC WHERE diagnosis_category = 'Cardiology'
# MAGIC ORDER BY readmission_rate DESC;
# MAGIC
# MAGIC 4. Which hospital is spending the most on high-readmission diseases?
# MAGIC SELECT hospital_name, diagnosis_category, total_cost
# MAGIC FROM anirvandecodes.gold.hospital_disease_kpi
# MAGIC ORDER BY total_cost DESC;