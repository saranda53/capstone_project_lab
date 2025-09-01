# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
catalog = "capstone"
silver, gold = "silver", "gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold}")

claims = spark.table(f"{catalog}.{silver}.st_claims")
providers = spark.table(f"{catalog}.{silver}.st_providers")

# scoring
scored = (claims
    .withColumn("FraudScore", 
        (F.col("Amount") > 10000).cast("int") * 3 +
        (F.col("DiagnosisCode").isNull() | (F.length("DiagnosisCode") == 0)).cast("int") * 2 +
        (F.col("ProviderID").isNull() | (F.col("MemberID").isNull())).cast("int") * 1
    )
)

(scored
 .join(providers.select("ProviderID","Specialties","Address","City", "State"), "ProviderID", "left")
 .write
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{gold}.gt_claims_scored")
)

# Aggregations for BI
agg = (scored
    .groupBy(F.to_date("ServiceDate").alias("ServiceDate"))
    .agg(F.countDistinct("ClaimID").alias("Claims"),
         F.sum("Amount").alias("totalAmount"),
         F.avg("FraudScore").alias("avgFraudScore"))
)

(agg.write.mode("overwrite").saveAsTable(f"{catalog}.{gold}.gt_daily_claims_summary"))

