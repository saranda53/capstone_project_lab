import dlt
from pyspark.sql import functions as F

@dlt.view
def claims_batch_raw():
    return (spark.read.format("delta").table("capstone.bronze.bt_claims_batch"))

@dlt.view
def claims_stream_raw():
    return (spark.read.format("delta").table("capstone.bronze.bt_claims_stream"))

@dlt.table(
    comment="Unified claims with expectations",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({
    "pk_not_null": "ClaimID IS NOT NULL",
    "member_not_null": "MemberID IS NOT NULL",
    "provider_not_null": "ProviderID IS NOT NULL"
})
def claims_clean():
    claims_union = (dlt.read("bt_claims_batch")
            .unionByName(dlt.read("bt_claims_stream"), allowMissingColumns=True))
    
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
    return claims_exploded

@dlt.table(comment="Diagnosis reference")
def diagnosis_ref():
    return spark.read.table("capstone.bronze.bt_diagnosis")

@dlt.table(comment="Valid claims")
def claims_valid():
    return dlt.read("claims_clean").join(dlt.read("diagnosis_ref"), "DiagnosisCode", "left_semi")


@dlt.table(comment="Invalid claims (failures)")
def claims_invalid():
    return dlt.read("claims_clean").join(dlt.read("diagnosis_ref"), "DiagnosisCode", "left_anti")

@dlt.table(comment="Gold: fraud-scored claims")
def claims_scored():
    df = dlt.read("claims_valid")
    score = (
        (F.col("Amount") > 10000).cast("int") * 3 +
        (F.col("DiagnosisCode").isNull() | (F.length("DiagnosisCode") == 0)).cast("int") * 2 +
        (F.col("ProviderID").isNull() | (F.col("MemberID").isNull())).cast("int") * 1
    )
    return df.withColumn("FraudScore", score)


