#!/usr/bin/env python3

import os
import glob
import shutil
from datetime import timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, max as spark_max, when, date_format,
    min as spark_min, coalesce, round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, TimestampType
)


class NetDiskSparkJob:
    """Encapsulates logic for network & disk alert analysis using PySpark."""

    def __init__(self, net_file="network_data.csv", disk_file="disk_data.csv", team_no="103"):
        self.net_file = net_file
        self.disk_file = disk_file
        self.team_no = team_no

        # Thresholds
        self.NET_THRESHOLD = 7205.16
        self.DISK_THRESHOLD = 4220.24
        self.WINDOW_SIZE_SEC = 30
        self.SLIDE_SEC = 10

        # Tracking structures
        self.data_files = {
            "network": self.net_file,
            "disk": self.disk_file
        }
        self.stats = {"net_rows": 0, "disk_rows": 0}
        self.debug_log = []
        self.spark = SparkSession.builder.appName("NET_DISK_Alert_Consumer2_OOP").getOrCreate()

        # Define schemas
        self.net_schema = StructType([
            StructField("ts", TimestampType(), True),
            StructField("server_id", StringType(), True),
            StructField("net_in", FloatType(), True)
        ])
        self.disk_schema = StructType([
            StructField("ts", TimestampType(), True),
            StructField("server_id", StringType(), True),
            StructField("disk_io", FloatType(), True)
        ])

    def _update_count(self, df, label):
        """Update count info."""
        cnt = df.count()
        self.stats[f"{label}_rows"] = cnt
        self._log(f"{label.upper()} data loaded with {cnt} rows.")

    def _load_data(self):
        net_df = self.spark.read.csv(self.net_file, header=True, schema=self.net_schema)
        disk_df = self.spark.read.csv(self.disk_file, header=True, schema=self.disk_schema)

        self._update_count(net_df, "net")
        self._update_count(disk_df, "disk")

        net_df = net_df.withColumn("net_in", round(col("net_in").cast(FloatType()), 2))
        disk_df = disk_df.withColumn("disk_io", round(col("disk_io").cast(FloatType()), 2))

        self._redundant_math(sum(self.stats.values()))  # no-op math

        return net_df, disk_df

    def _prepare_windows(self, combined_df):
        min_ts = combined_df.agg(spark_min("ts")).collect()[0][0]
        max_ts = combined_df.agg(spark_max("ts")).collect()[0][0]

        start_seconds = min_ts.second
        aligned_second = (start_seconds // self.SLIDE_SEC) * self.SLIDE_SEC
        start_time = min_ts.replace(second=aligned_second, microsecond=0)

        window_starts = []
        curr = start_time
        while curr <= max_ts:
            window_starts.append(curr)
            curr += timedelta(seconds=self.SLIDE_SEC)

        servers = [r["server_id"] for r in combined_df.select("server_id").distinct().collect()]
        window_rows = [
            Row(server_id=srv,
                window_start_ts=start,
                window_end_ts=start + timedelta(seconds=self.WINDOW_SIZE_SEC))
            for srv in servers
            for start in window_starts
        ]

        self._log(f"Generated {len(window_rows)} windows for {len(servers)} servers.")
        return self.spark.createDataFrame(window_rows).withColumnRenamed("server_id", "window_server_id")

    def _aggregate_alerts(self, joined_df):
        agg_df = joined_df.groupBy(
            coalesce(col("server_id"), col("window_server_id")).alias("server_id"),
            col("window_start_ts"),
            col("window_end_ts")
        ).agg(
            round(spark_max("net_in"), 2).alias("max_net_in"),
            round(spark_max("disk_io"), 2).alias("max_disk_io")
        )

        alerts_df = agg_df.withColumn(
            "alert",
            when((col("max_net_in") > self.NET_THRESHOLD) & (col("max_disk_io") > self.DISK_THRESHOLD),
                 "Network flood + Disk thrash suspected")
            .when((col("max_net_in") > self.NET_THRESHOLD) & (col("max_disk_io") <= self.DISK_THRESHOLD),
                  "Possible DDoS")
            .when((col("max_disk_io") > self.DISK_THRESHOLD) & (col("max_net_in") <= self.NET_THRESHOLD),
                  "Disk thrash suspected")
        )

        self._log("Aggregated and computed alerts successfully.")
        return alerts_df

    def _write_output(self, final_df):
        output_filename = f"team_{self.team_no}_NET_DISK.csv"
        tmp_dir = f"team_{self.team_no}_NET_DISK_tmp"

        (final_df
         .drop("window_start_ts")
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(tmp_dir))

        part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        if part_files:
            if os.path.exists(output_filename):
                os.remove(output_filename)
            shutil.move(part_files[0], output_filename)
        shutil.rmtree(tmp_dir, ignore_errors=True)

        self._log(f"Final CSV written to {output_filename}")
        return output_filename

    def run(self):
        net_df, disk_df = self._load_data()

        combined_df = net_df.join(disk_df, ["ts", "server_id"])
        self._log("Network and Disk data joined.")

        windows_df = self._prepare_windows(combined_df)

        joined = combined_df.join(
            windows_df,
            (combined_df.server_id == windows_df.window_server_id) &
            (combined_df.ts >= windows_df.window_start_ts) &
            (combined_df.ts < windows_df.window_end_ts),
            how="right"
        )

        alerts_df = self._aggregate_alerts(joined)

        final_output = alerts_df.select(
            col("server_id"),
            date_format(col("window_start_ts"), "HH:mm:ss").alias("window_start"),
            date_format(col("window_end_ts"), "HH:mm:ss").alias("window_end"),
            col("max_net_in"),
            col("max_disk_io"),
            col("alert"),
            col("window_start_ts")
        ).orderBy("server_id", "window_start_ts")

        output_filename = self._write_output(final_output)

        print(f"[DONE] Spark Job finished. Alerts saved to '{output_filename}'")
        print(f"[INFO] Data Stats: {self.stats}")
        print(f"[INFO] Logged Steps: {len(self.debug_log)} events")
        self.spark.stop()
        
if __name__ == "__main__":
    job = NetDiskSparkJob()
    job.run()
