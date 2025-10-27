#!/usr/bin/env python3

import sys
import os
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, min as spark_min, max as spark_max, count, input_file_name
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session(master_url):
    spark = (
        SparkSession.builder
        .appName("Spark_Cluster_Usage_Analysis")
        .master(master_url)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    # Configure S3 access from IAM role
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    return spark


def run_spark_job(master_url):
    # create spark session
    spark = create_spark_session(master_url)

    # Make output folder if it doesnt exist
    Path("data/output").mkdir(parents = True, exist_ok = True)

    # -----------------------------------
    # ---- Load raw log data from S3 ----
    # -----------------------------------

    s3_path = f"s3a://dc1524-assignment-spark-cluster-logs/data/*/*"
    df = spark.read.text(s3_path)

    # ----------------------
    # ---- Timeline CSV ----
    # ----------------------

    df_parsed = (
        df.withColumn("file_path", input_file_name())
          .withColumn("application_id", regexp_extract(col("file_path"), r"data/([^/]+)/", 1))
          .withColumn("cluster_id", regexp_extract(col("application_id"), r"application_(\d+)_\d+", 1))
          .filter(col("application_id") != "")
          .filter(col("cluster_id") != "")
    )

    # Extract timestamp from log line
    df_parsed = df_parsed.withColumn(
        "log_ts",
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    ).filter(col("log_ts") != "")

    # Convert log_ts to timestamp type in Spark
    df_parsed = df_parsed.withColumn(
        "log_ts",
        F.to_timestamp(col("log_ts"), "yy/MM/dd HH:mm:ss")
    )

    # --------------------------------------
    # ---- Compute start and end times ----
    # --------------------------------------
    timeline_df = (
        df_parsed.groupBy("application_id", "cluster_id")
                 .agg(
                     F.min("log_ts").alias("start_time"),
                     F.max("log_ts").alias("end_time")
                 )
                 .orderBy("cluster_id", "start_time")
    )

    # Add application number per cluster
    window = Window.partitionBy("cluster_id").orderBy("start_time")
    timeline_df = timeline_df.withColumn("app_number", F.row_number().over(window))
    timeline_df = timeline_df.withColumn("app_number", F.format_string("%04d", col("app_number")))

    # Convert small aggregated timeline table to Pandas for CSV/plotting
    timeline_pd = timeline_df.toPandas()
    timeline_pd.to_csv("data/output/problem2_timeline.csv", index=False)

    # -----------------------------
    # ---- Cluster Summary CSV ----
    # -----------------------------

    # Create clsuter summary bu grouping by cluster_id, counting applications, getting min start_time and max_end_time
    cluster_summary_df = (
        timeline_df.groupBy("cluster_id")
                   .agg(
                       F.count("application_id").alias("num_applications"),
                       F.min("start_time").alias("cluster_first_app"),
                       F.max("end_time").alias("cluster_last_app")
                   )
                   .orderBy(F.desc("num_applications"))
    )

    cluster_summary_pd = cluster_summary_df.toPandas()
    cluster_summary_pd.to_csv("data/output/problem2_cluster_summary.csv", index=False)


    # ------------------------------
    # ---- Cluster Summary Text ----
    # ------------------------------

    total_clusters = cluster_summary_pd["cluster_id"].nunique()
    total_apps = len(timeline_pd)
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0

    with open("data/output/problem2_stats.txt", "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for _, row in cluster_summary_pd.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

    # Stop spark job
    spark.stop()

    # Generate plots
    generate_plots()


def generate_plots():
    # Read csvs
    timeline_pd = pd.read_csv("data/output/problem2_timeline.csv")
    cluster_summary = pd.read_csv("data/output/problem2_cluster_summary.csv")

    # make output folder
    Path("data/output").mkdir(parents = True, exist_ok = True)

    # ---------------------------------------------
    # ----- Generate num_applications Barplot -----
    # ---------------------------------------------

    plt.figure(figsize = (10, 6))
    ax = sns.barplot(
        data = cluster_summary.sort_values("num_applications", ascending = False),
        x = "cluster_id",
        y = "num_applications"
    )

    for p in ax.patches:
        ax.annotate(format(p.get_height(), ".0f"),
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha = "center", va = "bottom")
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.xticks(rotation = 45)
    plt.tight_layout()
    plt.savefig("data/output/problem2_bar_chart.png")
    plt.close()

    # ----------------------------------------
    # ----- Generate Durations Histogram -----
    # ----------------------------------------

    # Count num applications in the largest cluster
    largest_cluster = (
        timeline_pd.groupby("cluster_id")["application_id"]
                   .count()
                   .sort_values(ascending = False)
                   .index[0]
    )

    # Get durations df
    largest_cluster = cluster_summary.sort_values("num_applications", ascending = False).iloc[0]["cluster_id"]
    durations = timeline_pd[timeline_pd["cluster_id"] == largest_cluster].copy()
    durations["duration_sec"] = (pd.to_datetime(durations["end_time"]) - pd.to_datetime(durations["start_time"])).dt.total_seconds()

    plt.figure(figsize = (10, 6))
    sns.histplot(durations["duration_sec"], kde = True)
    plt.xscale("log")
    plt.title(f"Job Duration Distribution for Cluster {largest_cluster} (n={len(durations)})")
    plt.xlabel("Duration (seconds, log scale)")
    plt.tight_layout()
    plt.savefig("data/output/problem2_density_plot.png")
    plt.close()





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-spark", action = "store_true",
                        help = "Regenerate plots using existing CSV files")
    parser.add_argument("master_url", nargs = "?",
                        help = "Spark master URL")
    args = parser.parse_args()

    if args.skip_spark:
        generate_plots()
    elif args.master_url:
        run_spark_job(args.master_url)
    else:
        parser.print_help()

"""
STEP TO RUN:

1. Ensure cluster-config.txt exists and load it:
    source cluster-config.txt

2. Copy problem2.py to the master node:
    scp -i $KEY_FILE problem2.py ubuntu@$MASTER_PUBLIC_IP:~/

3. SSH into the master node:
    ssh -i $KEY_FILE ubuntu@$MASTER_PUBLIC_IP

4. Run the Spark job:
    uv run python problem2.py spark://$MASTER_PRIVATE_IP:7077


4.5 Skip spark job and just generate plots:
    uv run python problem2.py --skip-spark

5. Copy results back to your local machine:
    exit

    scp -i $KEY_FILE -r ubuntu@$MASTER_PUBLIC_IP:~/data/output/* ./data/output
"""