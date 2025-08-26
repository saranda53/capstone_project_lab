# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
# COMMAND ----------
# define variable
catalog_name = "workspace"
schema_name = "	default"

# COMMAND ----------
# Define explicit schemas for enforcement
claim_schema = StructType([
    StructField("ClaimID", StringType(), True),
    StructField("PatientID", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("DiagnosisCode", StringType(), True),
    StructField("ProcedureCode", StringType(), True),
    StructField("ClaimAmount", DoubleType(), True),
    StructField("ClaimDate", DateType(), True),
    StructField("Status", StringType(), True),
])

diagnosis_schema = StructType([
    StructField("DiagnosisCode", StringType(), True),
    StructField("Description", StringType(), True),
])

patient_schema = StructType([
    StructField("PatientID", StringType(), True),
    StructField("PatientName", StringType(), True),
    StructField("DOB", DateType(), True),
    StructField("Gender", StringType(), True),
    StructField("Address", StringType(), True),
])


provider_schema = StructType([
    StructField("ProviderID", StringType(), True),
    StructField("ProviderName", StringType(), True),
    StructField("Specialty", StringType(), True),
    StructField("Location", StringType(), True),
])

# COMMAND ----------

input_path = "/Volumes/workspace/default/data/input"

claim_df = (spark.read
            .format("csv")
            .schema(claim_schema)
            .option("header", "true")
            .option("DateFormat", "yyyy-MM-dd")
            .load(f"{input_path}/claims.csv"))

diagnosis_df = (spark.read
               .format("csv")
               .schema(diagnosis_schema)
               .option("header", "true")
               .load(f"{input_path}/diagnosis_codes.csv"))

patient_df = (spark.read
                .format("csv")
                .schema(patient_schema)
                .option("header", "true")
                .option("DateFormat", "yyyy-MM-dd")
                .load(f"{input_path}/patients.csv"))

provider_df = (spark.read
                .format("csv")
                .schema(provider_schema)
                .option("header", "true")
                .option("DateFormat", "yyyy-MM-dd")
                .load(f"{input_path}/providers.csv"))


(claim_df.write.format("delta").mode("overwrite").saveAs(f"{catalog_name}.{schema_name}.bt_claim"))
(diagnosis_df.write.format("delta").mode("overwrite").save(f"{catalog_name}.{schema_name}.bt_diagnosis"))
(patient_df.write.format("delta").mode("overwrite").save(f"{catalog_name}.{schema_name}.bt_patient"))
(provider_df.write.format("delta").mode("overwrite").save(f"{catalog_name}.{schema_name}.bt_provider"))
