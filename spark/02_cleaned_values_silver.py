import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("BloodSilver")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.getenv("AWS_ACCESS_KEY_ID")
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "s3.us-east-2.amazonaws.com"
    )
    .getOrCreate()
)

df = (
    spark.readStream
    .format("delta")
    .load(
        "s3a://blood-supply-intelligence-lakehouse/bronze"
    )
)

silver_df = (
    df
    .withColumn(
        "inventory_status",
        when(col("current_inventory") < 50, "CRITICAL")
        .when(col("current_inventory") < 100, "LOW")
        .otherwise("HEALTHY")
    )
)

(
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://blood-supply-intelligence-lakehouse/checkpoints/silver"
    )
    .start(
        "s3a://blood-supply-intelligence-lakehouse/silver"
    )
    .awaitTermination()
)
