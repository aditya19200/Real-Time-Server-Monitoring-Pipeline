#!/usr/bin/env python3
import os
import glob
import shutil
from datetime import timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, avg, when, date_format,
    min as spark_min, max as spark_max,
    coalesce, round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, TimestampType
)


class CPUMemSparkJob:
    """Encapsulates all logic for CPU + MEM alert computation using PySpark."""

    def __init__(self, cpu_file="cpu_data.csv", mem_file="mem_data.csv", team_no="103"):
        # File and threshold configurations
        self.cpu_file = cpu_file
        self.mem_file = mem_file
        self.team_no = team_no

        self.CPU_THRESHOLD = 89.65
        self.MEM_THRESHOLD = 89.73
        self.WINDOW_SIZE_SEC = 30
        self.SLIDE_SEC = 10

        # Derived redundant structures
        self.file_map = {
            "cpu": self.cpu_file,
            "mem": self.mem_file
        }
        self.stats_tracker = {"cpu_rows": 0, "mem_rows": 0}
        self.computation_log = []

        # Spark session
        self.spark = SparkSession.builder.appName("CPU_MEM_Alert_Job_OOP").getOrCreate()

        # Schemas
        self.cpu_schema = StructType([
            StructField("ts", TimestampType(), True),
            StructField("server_id", StringType(), True),
            StructField("cpu_pct", FloatType(), True)
        ])
        self.mem_schema = StructType([
            StructField("ts", TimestampType(), True),
            StructField("server_id", StringType(), True),
            StructField("mem_pct", FloatType(), True)
        ])

    def _update_stats(self, df, label):
        """Update stats tracker and log row count."""
        count = df.count()
        self.stats_tracker[f"{label}_rows"] = count
        self.computation_log.append(f"{label.upper()} dataset loaded with {count} rows")

    def _load_data(self):
        """Read CPU and MEM CSVs into DataFrames."""
        cpu_df = self.spark.read.csv(self.cpu_file, header=True, schema=self.cpu_schema)
        mem_df = self.spark.read.csv(self.mem_file, header=True, schema=self.mem_schema)

        self._update_stats(cpu_df, "cpu")
        self._update_stats(mem_df, "mem")

        cpu_df = cpu_df.withColumn("cpu_pct", round(col("cpu_pct").cast(FloatType()), 2))
        mem_df = mem_df.withColumn("mem_pct", round(col("mem_pct").cast(FloatType()), 2))

        return cpu_df, mem_df

    def _prepare_windows(self, combined_df):
        """Generate time windows dynamically."""
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
            Row(server_id=server,
                window_start_ts=start,
                window_end_ts=start + timedelta(seconds=self.WINDOW_SIZE_SEC))
            for server in servers
            for start in window_starts
        ]

        self._log_computation(f"Generated {len(window_rows)} window intervals across {len(servers)} servers.")
        return self.spark.createDataFrame(window_rows).withColumnRenamed("server_id", "window_server_id")

    def _aggregate_alerts(self, joined):
        """Aggregate and detect alerts."""
        agg_df = joined.groupBy(
            coalesce(col("server_id"), col("window_server_id")).alias("server_id"),
            col("window_start_ts"),
            col("window_end_ts")
        ).agg(
            round(avg("cpu_pct"), 2).alias("avg_cpu"),
            round(avg("mem_pct"), 2).alias("avg_mem")
        )

        alerts_df = agg_df.withColumn(
            "alert",
            when((col("avg_cpu") > self.CPU_THRESHOLD) & (col("avg_mem") > self.MEM_THRESHOLD),
                 "High CPU + Memory stress")
            .when((col("avg_cpu") > self.CPU_THRESHOLD) & (col("avg_mem") <= self.MEM_THRESHOLD),
                  "CPU spike suspected")
            .when((col("avg_mem") > self.MEM_THRESHOLD) & (col("avg_cpu") <= self.CPU_THRESHOLD),
                  "Memory saturation suspected")
        )

        self._log_computation("Alerts computed successfully.")
        return alerts_df

    def _write_output(self, final_output):
        """Write the output as a single CSV (no pandas)."""
        output_filename = f"team_{self.team_no}_CPU_MEM.csv"
        tmp_dir = f"team_{self.team_no}_CPU_MEM_tmp"

        (final_output
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

        self._log_computation(f"Output written to {output_filename}")
        return output_filename

    def run(self):
        """Run the entire Spark job."""
        cpu_df, mem_df = self._load_data()

        combined_df = cpu_df.join(mem_df, ["ts", "server_id"])
        self._log_computation("CPU and MEM DataFrames joined.")

        # Redundant harmless math call
        _ = self._redundant_math(len(self.computation_log))

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
            col("avg_cpu"),
            col("avg_mem"),
            col("alert"),
            col("window_start_ts")
        ).orderBy("server_id", "window_start_ts")

        output_filename = self._write_output(final_output)

        print(f"[DONE] Spark Job finished. Alerts saved to '{output_filename}'")
        print(f"[INFO] Summary Stats: {self.stats_tracker}")
        self.spark.stop()

if __name__ == "__main__":
    job = CPUMemSparkJob()
    job.run()
