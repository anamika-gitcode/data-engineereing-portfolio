"""
Azure Databricks: Delta Table Processing & Optimization Job
--------------------------------------------------------------
Reads raw Delta data, repartitions to avoid the small-file problem,
writes filtered/cleaned output to a target Delta table, and runs
OPTIMIZE with Z-ORDER to improve downstream query performance.

Author: Ana
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("databricks_job")


def optimize_and_process(spark: SparkSession, source_path: str, target_table: str):
    """
    Load source Delta data, repartition for balanced parallelism,
    filter to active records, write to target table, and optimize.
    """
    df = spark.read.format("delta").load(source_path)

    # Repartition to avoid small-file problem; log counts for monitoring
    df = df.repartition(200, col("partition_key"))
    row_count = df.count()
    logger.info(f"Processing {row_count} rows across {df.rdd.getNumPartitions()} partitions")

    df.filter(col("status") == "active") \
        .write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (customer_id)")
    logger.info(f"Job completed and table optimized: {target_table}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("delta-optimize-job").getOrCreate()
    optimize_and_process(
        spark=spark,
        source_path="/mnt/raw/orders",
        target_table="analytics.orders_active",
    )
