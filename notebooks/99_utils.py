# Databricks notebook source
# Databricks notebook source
### Capstone Project Utilities (paths, schemas, helpers)

from pyspark.sql import functions as F

def bronze_paths(catalog='capstone', schema='bronze', volume='raw'):
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    return {
        'claims_batch': f"{base}/claims_batch/",
        'claims_stream': f"{base}/claims_stream/",
        'providers_json': f"{base}/providers/",
        'diagnosis_ref': f"{base}/diagnosis_ref/",
        'members_csv': f"{base}/members/",
        'checkpoint': f"{base}/_checkpoints/"
    }

def table_name(catalog, schema, name):
    return f'{catalog}.{schema}.{name}'

def with_ingest_meta(df, source):
    return (df
        .withColumn("_ingest_source", F.lit(source))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
    )
