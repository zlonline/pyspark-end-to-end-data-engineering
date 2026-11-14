
import csv
import statistics
import time

from pyspark.sql import functions as F

from week12.config import (
    ROOT_DIR,
    GOLD_DIR,
    SILVER_DIR
)
from week12.ingestion import create_spark


RESULTS_DIR = ROOT_DIR / "week12" / "results"

CUSTOMER_SALES_PATH = (
    GOLD_DIR / "customer_sales"
)

SALES_DETAIL_PATH = (
    SILVER_DIR / "sales_detail"
)

BENCHMARK_DIR = (
    GOLD_DIR / "partition_benchmark"
)

RUNS = 3


# ========================================
# 1. Count Parquet Files
# ========================================

def count_parquet_files(path):
    return len(
        list(path.rglob("*.parquet"))
    )


# ========================================
# 2. Detect Hot Keys
# ========================================

def detect_data_skew(spark):
    df = spark.read.parquet(
        str(SALES_DETAIL_PATH)
    )

    print("\n=== TOP CUSTOMER KEYS ===")

    key_counts = (
        df.groupBy("customer_id")
        .count()
        .orderBy(F.desc("count"))
    )

    key_counts.show(10)

    stats = key_counts.agg(
        F.max("count").alias("max_count"),
        F.avg("count").alias("avg_count")
    ).first()

    max_count = stats["max_count"]
    avg_count = stats["avg_count"]

    skew_ratio = (
        max_count / avg_count
        if avg_count else 0
    )

    print("Max Key Count:", max_count)
    print("Avg Key Count:", round(avg_count, 2))
    print("Max / Avg:", round(skew_ratio, 2))

    return skew_ratio


# ========================================
# 3. Partition Distribution
# ========================================

def analyze_partitions(df):
    distribution = (
        df.withColumn(
            "partition_id",
            F.spark_partition_id()
        )
        .groupBy("partition_id")
        .count()
        .orderBy("partition_id")
    )

    print("\n=== PARTITION DISTRIBUTION ===")
    distribution.show(100, truncate=False)

    rows = distribution.collect()

    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output = (
        RESULTS_DIR /
        "partition_distribution.csv"
    )

    with output.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(
            ["partition_id", "row_count"]
        )

        for row in rows:
            writer.writerow([
                row["partition_id"],
                row["count"]
            ])

    return rows


# ========================================
# 4. Partition Strategies
# ========================================

def apply_strategy(df, strategy):
    if strategy == "default":
        return df

    if strategy == "repartition_4":
        return df.repartition(4)

    if strategy == "repartition_8":
        return df.repartition(8)

    if strategy == "coalesce_2":
        return df.coalesce(2)

    raise ValueError(
        f"Unknown strategy: {strategy}"
    )


# ========================================
# 5. Benchmark One Run
# ========================================

def benchmark_one(
    spark,
    strategy,
    run_number
):
    source = spark.read.parquet(
        str(CUSTOMER_SALES_PATH)
    )

    df = apply_strategy(
        source,
        strategy
    )

    output = (
        BENCHMARK_DIR /
        strategy /
        f"run_{run_number}"
    )

    start = time.perf_counter()

    df.write.mode("overwrite").parquet(
        str(output)
    )

    elapsed = time.perf_counter() - start

    file_count = count_parquet_files(
        output
    )

    print(
        f"{strategy} Run {run_number}: "
        f"{elapsed:.3f}s, "
        f"files={file_count}"
    )

    return {
        "strategy": strategy,
        "run": run_number,
        "elapsed_seconds": round(
            elapsed, 3
        ),
        "file_count": file_count
    }


# ========================================
# 6. Partition Pruning
# ========================================

def test_partition_pruning(spark):
    source = spark.read.parquet(
        str(GOLD_DIR / "daily_sales")
    )

    output = (
        GOLD_DIR / "daily_sales_partitioned"
    )

    source.write.mode("overwrite") \
        .partitionBy("sales_year") \
        .parquet(str(output))

    df = spark.read.parquet(
        str(output)
    )

    filtered = df.filter(
        F.col("sales_year") == 2025
    )

    print("\n=== PARTITION PRUNING PLAN ===")
    filtered.explain("formatted")

    print(
        "2025 Daily Rows:",
        filtered.count()
    )


# ========================================
# 7. Main
# ========================================

def main():
    spark = create_spark(
        "Week12Day04PartitionStrategy"
    )

    try:
        RESULTS_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        detect_data_skew(spark)

        source = spark.read.parquet(
            str(CUSTOMER_SALES_PATH)
        )

        analyze_partitions(source)

        strategies = [
            "default",
            "repartition_4",
            "repartition_8",
            "coalesce_2"
        ]

        results = []

        # Rotate strategy order between runs.
        for run_number in range(1, RUNS + 1):
            offset = (run_number - 1) % len(
                strategies
            )

            sequence = (
                strategies[offset:] +
                strategies[:offset]
            )

            for strategy in sequence:
                result = benchmark_one(
                    spark,
                    strategy,
                    run_number
                )

                results.append(result)

        # Write raw results.
        csv_path = (
            RESULTS_DIR /
            "partition_benchmark.csv"
        )

        with csv_path.open(
            "w",
            newline="",
            encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "strategy",
                    "run",
                    "elapsed_seconds",
                    "file_count"
                ]
            )

            writer.writeheader()
            writer.writerows(results)

        print("\n=== MEDIAN BENCHMARK ===")

        for strategy in strategies:
            times = [
                r["elapsed_seconds"]
                for r in results
                if r["strategy"] == strategy
            ]

            files = [
                r["file_count"]
                for r in results
                if r["strategy"] == strategy
            ]

            print(
                strategy,
                "median_sec=",
                round(statistics.median(times), 3),
                "median_files=",
                statistics.median(files)
            )

        test_partition_pruning(spark)

        print("\nBenchmark CSV:", csv_path)

        print("\nSpark UI: http://localhost:4040")
        input("\nPress Enter to stop Spark...")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
