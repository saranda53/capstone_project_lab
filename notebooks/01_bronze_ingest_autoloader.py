# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType, BooleanType, ArrayType
# import dlt

# COMMAND ----------

# define variable
catalog = "capstone"
schema = "bronze"
volume="raw"

base = f"/Volumes/{catalog}/{schema}/{volume}"

input_claim_batch = f"{base}/claims_batch/"
input_claim_stream = f"{base}/claims_stream/"
input_providers = f"{base}/providers/"
input_diagnosis =  f"{base}/diagnosis_ref/"
input_members = f"{base}/members/"
checkpoint =  f"{base}/checkpoints/"
schema_location = f"{base}/schemas/"


# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")


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



claim_df = (spark.read
            .format("csv")
            .schema(claim_schema)
            .option("header", "true")
            .option("DateFormat", "yyyy-MM-dd")
            .option("TimestampFormat", "yyyy-MM-dd hh:mm:ss")
            .load(f"{input_claim_batch}/claims_batch.csv"))

diagnosis_df = (spark.read
               .format("csv")
               .schema(diagnosis_schema)
               .option("header", "true")
               .load(f"{input_diagnosis}/diagnosis_ref.csv"))

member_df = (spark.read
                .format("csv")
                .schema(member_schema)
                .option("header", "true")
                .option("DateFormat", "yyyy-MM-dd")
                .load(f"{input_members}/members.csv"))

provider_df = (spark.read
                .format("json")
                .schema(provider_schema)
                .option("header", "true")
                .option("DateFormat", "yyyy-MM-dd")
                .load(f"{input_providers}/providers.json"))




# COMMAND ----------

(claim_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bt_claims_batch"))
(diagnosis_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bt_diagnosis"))
(member_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bt_members"))
(provider_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bt_providers"))

# COMMAND ----------

# claim_stream_df = (spark.read
#             .format("json")
#             .schema(claim_schema)
#             .option("header", "true")
#             .option("DateFormat", "yyyy-MM-dd")
#             .option("TimestampFormat", "yyyy-MM-dd hh:mm:ss")
#             .load(f"{input_claim_stream}/claims_stream.json"))

# (claim_stream_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bt_claims_stream"))

# COMMAND ----------

claim_stream_df = (
  spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("header", True)
      .option("cloudFiles.schemaLocation", schema_location)
      .option("pathGlobFilter", "claims_stream.json")
      .schema(claim_schema)
      .load(input_claim_stream)
)

query = (
  claim_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bt_claims_stream")
)

# COMMAND ----------

# # COPY INTO
# batch_path = bronze_paths(catalog, bronze_schema)['claims_batch']
# spark.sql(f'''
# COPY INTO {table_name(catalog, bronze_schema, "bt_claims")}
# FROM '{batch_path}'
# FILEFORMAT = CSV
# FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# COPY_OPTIONS ('mergeSchema' = 'true')
# ''')
