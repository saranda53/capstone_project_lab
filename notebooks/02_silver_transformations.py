# Databricks notebook source
# Databricks notebook source

from pyspark.sql import functions as F, types as T

catalog = "capstone"
bronze, silver = "bronze", "silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver}")

claims_batch_raw = f"{catalog}.{bronze}.bt_claims_batch"
claims_stream_raw = f"{catalog}.{bronze}.bt_claims_stream"
providers_raw = f"{catalog}.{bronze}.bt_providers"
members_raw = f"{catalog}.{bronze}.bt_members"
diag_raw = f"{catalog}.{bronze}.bt_diagnosis"

# COMMAND ----------
# Claims Union batch + streaming
claims_union = spark.table(claims_batch_raw).unionByName(spark.table(claims_stream_raw), allowMissingColumns=True)

# Data Clean for Claim
claims_clean = (claims_union
    .withColumn("ICD10Codes_array", F.split("ICD10Codes", ";"))
    .filter(F.col("ClaimID").isNotNull() & F.col("MemberID").isNotNull() & F.col("ProviderID").isNotNull())
    .dropDuplicates(["ClaimID"])
)

# Explode ICD10Codes (DiagnosisCode) into multiple rows
claims_exploded = (claims_clean
                   .withColumn("DiagnosisCode", F.explode("ICD10Codes_array"))
                   .drop("ICD10Codes_array")   # drop array column
                )

# Foreign key validation via joins
diag = spark.table(diag_raw).select(F.col("Code").alias("DiagnosisCode"))
claims_cleans = claims_exploded.join(diag, "DiagnosisCode", "left_semi")
claims_rejects = claims_exploded.join(diag, "DiagnosisCode", "left_anti")

(claims_cleans.write.mode("overwrite").saveAsTable(f"{catalog}.{silver}.st_claims"))
(claims_rejects.write.mode("overwrite").saveAsTable(f"{catalog}.{silver}.st_claims_rejects"))

# COMMAND ----------
# Providers — parse nested structures
prov = spark.table(providers_raw)
flat = (prov
    .selectExpr("ProviderID",
                "Name",
                "explode_outer(Specialties) as Specialties",
                "Locations.Address as Address",
                "Locations.City as City",
                "Locations.State as State",
                "IsActive",
                "TIN",
                "LastVerified"
    )
)

(flat.write.mode("overwrite").saveAsTable(f"{catalog}.{silver}.st_providers"))

