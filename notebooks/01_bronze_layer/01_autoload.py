# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType, BooleanType, ArrayType
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# define variable
catalog_name = "workspace"
schema_name = "	default"

input_path = "/Volumes/workspace/default/data/input"
checkpoint_path = "/FileStore/data/checkpoints/autoloader_demo/"
schema_location = "/FileStore/data/schema/autoloader_demo/"

# COMMAND ----------

# Define explicit schemas for enforcement
claim_schema = StructType([
    StructField("ClaimID", StringType(), True),
    StructField("MemberID", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("ClaimDate", DateType(), True),
    StructField("ServiceDate", DateType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Status", StringType(), True),
    StructField("ICD10Codes", StringType(), True),
    StructField("CPTCodes", StringType(), True),
    StructField("ClaimType", StringType(), True),
    StructField("SubmissionChannel", StringType(), True),
    StructField("Notes", StringType(), True),
    StructField("IngestTimestamp", TimestampType(), True),
])

diagnosis_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Description", StringType(), True),
])

member_schema = StructType([
    StructField("MemberID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("DOB", DateType(), True),
    StructField("Gender", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("PlanType", StringType(), True),
    StructField("EffectiveDate", DateType(), True),
    StructField("Email", StringType(), True),
    StructField("IsActive", BooleanType(), True),
    StructField("LastUpdated", DateType(), True),
])

provider_schema = StructType([
    StructField("ProviderID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Specialties", ArrayType(StringType()), True),
    StructField("Locations", ArrayType(
        StructType([
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True)
        ])
    ), True),
    StructField("IsActive", BooleanType(), True),
    StructField("TIN", StringType(), True),
    StructField("LastVerified", DateType(), True)
])


# COMMAND ----------



# claim_df = (spark.read
#             .format("csv")
#             .schema(claim_schema)
#             .option("header", "true")
#             .option("DateFormat", "yyyy-MM-dd")
#             .option("TimestampFormat", "yyyy-MM-dd hh:mm:ss")
#             .load(f"{input_path}/claims_batch.csv"))

diagnosis_df = (spark.read
               .format("csv")
               .schema(diagnosis_schema)
               .option("header", "true")
               .load(f"{input_path}/diagnosis_ref.csv"))

member_df = (spark.read
                .format("csv")
                .schema(member_schema)
                .option("header", "true")
                .option("DateFormat", "yyyy-MM-dd")
                .load(f"{input_path}/members.csv"))

provider_df = (spark.read
                .format("json")
                .schema(provider_schema)
                .option("header", "true")
                .option("DateFormat", "yyyy-MM-dd")
                .load(f"{input_path}/providers.json"))




# COMMAND ----------

# (claim_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.bt_claim"))
(diagnosis_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.bt_diagnosis"))
(member_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.bt_member"))
(provider_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.bt_provider"))

# COMMAND ----------

claim_df = (
  spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("header", True)
      .option("cloudFiles.schemaLocation", schema_location)
      .option("pathGlobFilter", "claims_stream*.json")
      .schema(claim_schema)
      .load(input_path)
)

query = (
  claim_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(f"{catalog_name}.{schema_name}.bt_claim")
)
