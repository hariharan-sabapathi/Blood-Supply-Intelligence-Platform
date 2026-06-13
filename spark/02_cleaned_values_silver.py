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
    .getOrCreate()
)

df = (
    spark.readStream
    .format("delta")
    .load("/data/bronze")
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
        "/data/checkpoints/silver"
    )
    .start("/data/silver")
    .awaitTermination()
)