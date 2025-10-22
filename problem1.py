#!/usr/bin/env python3

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, rand, count, desc
import pandas as pd
from pathlib import Path


def create_spark_session(master_url):
    """Create Spark session"""
    spark = (
        SparkSession.builder
        .appName("Spark_Log_Level_Distribution")
        .master(master_url)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    # Configure S3 access via IAM role
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    return spark


def run_spark_job(master_url):
    """Analyze log level distribution across Spark application logs"""
    spark = create_spark_session(master_url)


    # -----------------------------------
    # ---- Load raw log data from S3 ----
    # -----------------------------------

    # Get the s3 path
    s3_path = f"s3a://dc1524-assignment-spark-cluster-logs/data/*/*"

    # Load the log data
    df = spark.read.text(s3_path)

    # Get the count of total lines
    total_lines = df.count()


    # ------------------------------------------------
    # ---- Extract log level using regex patterns ----
    # ------------------------------------------------
    
    # Define the regex pattern to extract log levels
    log_pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"

    # Get log levels from log entries
    df_logs = df.withColumn("log_level", regexp_extract(col("value"), log_pattern, 1))

    # Filter out invalid log levels
    df_valid = df_logs.filter(col("log_level") != "")

    # Count the total number of lines for valid log levels
    lines_with_levels = df_valid.count()


    # ----------------------------
    # ---- Count by log level ----
    # ----------------------------

    # Aggregate counts by log level
    counts_df = (
        df_valid.groupBy("log_level")
                .agg(count("*").alias("count"))
                .orderBy(desc("count"))
    )

    # Conver to pandas df
    counts_pd = counts_df.toPandas()

    # Create the output directory for csv files
    Path("data/output").mkdir(parents = True, exist_ok = True)

    # Save to csv
    counts_pd.to_csv("data/output/problem1_counts.csv", index = False)


    # ----------------------------------
    # ---- Random Sample of Entries ----
    # ----------------------------------

    # Get sample of 10 log entries
    sample_df = df_valid.orderBy(rand()).limit(10)

    # Conver the pandas df
    sample_pd = sample_df.select(col("value").alias("log_entry"), col("log_level")).toPandas()

    # Save to csv
    sample_pd.to_csv("data/output/problem1_sample.csv", index = False)


    # ------------------------------
    # ---- Summary text output -----
    # ------------------------------

    # Get all summary statistics
    unique_levels = counts_pd["log_level"].nunique()

    # Initialize summary lines
    summary_lines = [
        f"Total log lines processed: {total_lines}",
        f"Total lines with log levels: {lines_with_levels}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:"
    ]

    # Sum all valid log level counts
    total_valid = counts_pd["count"].sum()

    # Loop through each log level and calculate percentage distribution
    for _, row in counts_pd.iterrows():
        level = row["log_level"]
        count_val = row["count"]
        pct = (count_val / total_valid) * 100
        summary_lines.append(f"  {level:<6}: {count_val:>10,} ({pct:6.2f}%)")

    # Save summary text to a file
    with open("data/output/problem1_summary.txt", "w") as f:
        f.write("\n".join(summary_lines))

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1].startswith("spark://"):
        run_spark_job(sys.argv[1])
    else:
        print("Usage:")
        print("  python3 problem1.py <spark_master_url>")


"""
HOW TO RUN:
1. Ensure cluster-config.txt exists:
    source cluster-config.txt

2. Copy problem1.py to the master node
    scp -i $KEY_FILE problem1.py ubuntu@$MASTER_PUBLIC_IP:~/

3. SSH into the master node
    ssh -i $KEY_FILE ubuntu@$MASTER_PUBLIC_IP

4. Run the spark job
    uv run python problem1.py spark://$MASTER_PRIVATE_IP:7077



5. Copy files to local machine:
    exit 

    scp -i $KEY_FILE -r ubuntu@$MASTER_PUBLIC_IP:~/data/output ./data/output
"""