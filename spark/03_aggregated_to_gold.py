from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("BloodGold")
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

silver_df = (
    spark.readStream
    .format("delta")
    .load("/data/silver")
)

gold_df = (
    silver_df
    .groupBy(
        "blood_type",
        "city",
        "inventory_status"
    )
    .agg(
        avg("current_inventory").alias("avg_inventory"),
        sum("units_used").alias("total_units_used"),
        count("*").alias("event_count")
    )
)

(
    gold_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "/data/checkpoints/gold"
    )
    .start("/data/gold")
    .awaitTermination()
)