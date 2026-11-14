
import csv
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from week12.config import (
    ROOT_DIR,
    SILVER_DIR,
    SPARK_MASTER
)

from week12.transform import (
    read_clean_data,
    transform_orders,
    select_customers,
    select_products,
    build_sales_detail
)

from week12.aggregation import (
    aggregate_customer_sales,
    aggregate_product_sales,
    aggregate_daily_sales
)


RESULTS_DIR = ROOT_DIR / "week12" / "results"
EVENT_DIR = RESULTS_DIR / "spark-events"

OUTPUT_ROOT = (
    ROOT_DIR / "data" / "week12" /
    "gold" / "performance"
)

RUNS = 3

DATASETS = [
    "customer_sales",
    "product_sales",
    "daily_sales"
]


# ========================================
# 1. Spark Session
# ========================================

def create_benchmark_spark(name):
    RESULTS_DIR.mkdir(
        parents=True, exist_ok=True
    )

    EVENT_DIR.mkdir(
        parents=True, exist_ok=True
    )

    spark = (
        SparkSession.builder
        .appName(name)
        .master(SPARK_MASTER)
        .config(
            "spark.sql.shuffle.partitions", "8"
        )
        .config(
            "spark.sql.adaptive.enabled", "false"
        )
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            "-1"
        )
        .config(
            "spark.eventLog.enabled", "true"
        )
        .config(
            "spark.eventLog.dir",
            EVENT_DIR.resolve().as_uri()
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ========================================
# 2. Read and Transform
# ========================================

def build_sales(spark, optimized):
    orders, customers, products = (
        read_clean_data(spark)
    )

    orders = transform_orders(orders)
    customers = select_customers(customers)
    products = select_products(products)

    if optimized:
        customers = broadcast(customers)
        products = broadcast(products)
    else:
        orders = orders.hint("merge")
        customers = customers.hint("merge")
        products = products.hint("merge")

    return build_sales_detail(
        orders, customers, products
    )


# ========================================
# 3. Gold Aggregation
# ========================================

def build_gold(sales_detail):
    return {
        "customer_sales":
            aggregate_customer_sales(
                sales_detail
            ),

        "product_sales":
            aggregate_product_sales(
                sales_detail
            ),

        "daily_sales":
            aggregate_daily_sales(
                sales_detail
            )
    }


# ========================================
# 4. Output Strategy
# ========================================

def prepare_output(df, name, optimized):
    if not optimized:
        return df

    if name == "customer_sales":
        return df.coalesce(4)

    if name == "product_sales":
        return df.coalesce(2)

    if name == "daily_sales":
        return df.coalesce(1)

    return df


# ========================================
# 5. File Statistics
# ========================================

def file_stats(path):
    files = list(path.rglob("*.parquet"))

    total_bytes = sum(
        p.stat().st_size for p in files
    )

    return {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "size_mb": round(
            total_bytes / (1024 * 1024), 3
        )
    }


# ========================================
# 6. Save Execution Plan
# ========================================

def save_plan(df, name):
    path = RESULTS_DIR / f"{name}_plan.txt"

    plan = (
        df._jdf.queryExecution()
        .executedPlan()
        .toString()
    )

    path.write_text(
        plan, encoding="utf-8"
    )


# ========================================
# 7. Run One Pipeline
# ========================================

def run_pipeline(name, run_number):
    optimized = name == "optimized"

    app_name = (
        f"Week12Day05_{name}_Run{run_number}"
    )

    spark = create_benchmark_spark(
        app_name
    )

    cached = None

    try:
        start = time.perf_counter()

        sales = build_sales(
            spark, optimized
        )

        if run_number == 1:
            save_plan(sales, name)

        if optimized:
            sales = sales.persist()
            cached = sales

            # Materialize before downstream reuse.
            sales.count()

        gold = build_gold(sales)

        output_metrics = []

        for dataset_name in DATASETS:
            df = prepare_output(
                gold[dataset_name],
                dataset_name,
                optimized
            )

            partition_count = (
                df.rdd.getNumPartitions()
            )

            output = (
                OUTPUT_ROOT /
                name /
                f"run_{run_number}" /
                dataset_name
            )

            (
                df.write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(str(output))
            )

            stats = file_stats(output)

            output_metrics.append({
                "pipeline": name,
                "run": run_number,
                "dataset": dataset_name,
                "partitions": partition_count,
                **stats
            })

        elapsed = time.perf_counter() - start

        print(
            f"{name} Run {run_number}: "
            f"{elapsed:.3f} sec"
        )

        return {
            "pipeline": name,
            "run": run_number,
            "elapsed_seconds": round(
                elapsed, 3
            )
        }, output_metrics

    finally:
        if cached is not None:
            cached.unpersist()

        spark.stop()


# ========================================
# 8. Read Spark Event Logs
# ========================================

def parse_event_logs():
    rows = []

    if not EVENT_DIR.exists():
        return rows

    for path in EVENT_DIR.iterdir():
        if not path.is_file():
            continue

        # Ignore incomplete logs.
        if path.name.endswith(".inprogress"):
            continue

        app_name = None
        shuffle_read = 0
        shuffle_write = 0
        disk_spill = 0
        task_count = 0

        try:
            with path.open(
                "r", encoding="utf-8"
            ) as file:
                for line in file:
                    event = json.loads(line)

                    event_type = event.get("Event")

                    if event_type == (
                        "SparkListenerApplicationStart"
                    ):
                        app_name = event.get(
                            "App Name"
                        )

                    elif event_type == (
                        "SparkListenerTaskEnd"
                    ):
                        metrics = event.get(
                            "Task Metrics", {}
                        )

                        read_metrics = metrics.get(
                            "Shuffle Read Metrics", {}
                        )

                        write_metrics = metrics.get(
                            "Shuffle Write Metrics", {}
                        )

                        shuffle_read += (
                            read_metrics.get(
                                "Remote Bytes Read", 0
                            ) +
                            read_metrics.get(
                                "Local Bytes Read", 0
                            )
                        )

                        shuffle_write += (
                            write_metrics.get(
                                "Shuffle Bytes Written", 0
                            )
                        )

                        disk_spill += metrics.get(
                            "Disk Bytes Spilled", 0
                        )

                        task_count += 1

        except (
            OSError,
            UnicodeError,
            json.JSONDecodeError
        ):
            continue

        if not app_name:
            continue

        if not app_name.startswith(
            "Week12Day05_"
        ):
            continue

        rows.append({
            "app_name": app_name,
            "tasks": task_count,
            "shuffle_read_mb": round(
                shuffle_read / (1024 ** 2), 3
            ),
            "shuffle_write_mb": round(
                shuffle_write / (1024 ** 2), 3
            ),
            "disk_spill_mb": round(
                disk_spill / (1024 ** 2), 3
            )
        })

    return rows


# ========================================
# 9. Validate Output Consistency
# ========================================

def validate_results():
    spark = create_benchmark_spark(
        "Week12Day05_Validation"
    )

    try:
        for dataset in DATASETS:
            baseline_path = (
                OUTPUT_ROOT /
                "baseline" /
                "run_1" /
                dataset
            )

            optimized_path = (
                OUTPUT_ROOT /
                "optimized" /
                "run_1" /
                dataset
            )

            baseline = spark.read.parquet(
                str(baseline_path)
            )

            optimized = spark.read.parquet(
                str(optimized_path)
            )

            # exceptAll checks values and duplicates.
            difference_1 = (
                baseline.exceptAll(
                    optimized
                ).count()
            )

            difference_2 = (
                optimized.exceptAll(
                    baseline
                ).count()
            )

            if difference_1 or difference_2:
                raise AssertionError(
                    f"{dataset} results differ"
                )

            print(
                f"{dataset}: CONSISTENCY PASSED"
            )

    finally:
        spark.stop()


# ========================================
# 10. Write CSV
# ========================================

def write_csv(path, rows):
    if not rows:
        return

    with path.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(rows[0].keys())
        )

        writer.writeheader()
        writer.writerows(rows)


# ========================================
# 11. Write Markdown Report
# ========================================

def write_report(
    benchmarks,
    output_metrics,
    spark_metrics
):
    baseline = [
        r["elapsed_seconds"]
        for r in benchmarks
        if r["pipeline"] == "baseline"
    ]

    optimized = [
        r["elapsed_seconds"]
        for r in benchmarks
        if r["pipeline"] == "optimized"
    ]

    baseline_median = statistics.median(
        baseline
    )

    optimized_median = statistics.median(
        optimized
    )

    improvement = (
        (baseline_median - optimized_median)
        / baseline_median * 100
    )

    speedup = (
        baseline_median / optimized_median
    )

    lines = [
        "# Week 12 Day 5 - Performance Results",
        "",
        "## Benchmark Scope",
        "",
        "Clean Silver -> JOIN -> Gold Aggregations",
        "",
        "## Execution Time",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Baseline Median | {baseline_median:.3f} sec |",
        f"| Optimized Median | {optimized_median:.3f} sec |",
        f"| Improvement | {improvement:.2f}% |",
        f"| Speedup | {speedup:.2f}x |",
        "",
        "## Optimization Changes",
        "",
        "- Broadcast Customers and Products",
        "- Cache Sales Detail for repeated reuse",
        "- Tune output partition counts",
        "- Keep Snappy compression consistent",
        "",
        "## Correctness",
        "",
        "Baseline and Optimized outputs validated",
        "using bidirectional exceptAll.",
        "",
        "## Spark Metrics",
        "",
        "| Application | Tasks | Shuffle Read MB | "
        "Shuffle Write MB | Disk Spill MB |",
        "|---|---:|---:|---:|---:|"
    ]

    for row in spark_metrics:
        lines.append(
            f"| {row['app_name']} "
            f"| {row['tasks']} "
            f"| {row['shuffle_read_mb']} "
            f"| {row['shuffle_write_mb']} "
            f"| {row['disk_spill_mb']} |"
        )

    lines += [
        "",
        "## Output Files",
        "",
        "| Pipeline | Run | Dataset | Partitions | "
        "Files | Size MB |",
        "|---|---:|---|---:|---:|---:|"
    ]

    for row in output_metrics:
        lines.append(
            f"| {row['pipeline']} "
            f"| {row['run']} "
            f"| {row['dataset']} "
            f"| {row['partitions']} "
            f"| {row['file_count']} "
            f"| {row['size_mb']} |"
        )

    lines += [
        "",
        "## Observations",
        "",
        "- Compare Spark UI SQL and Stages tabs.",
        "- Identify Exchange and BroadcastHashJoin.",
        "- Review task skew and spill metrics.",
        "- Note that cache may add overhead.",
        "- Avoid assuming every optimization is faster.",
        "",
        "## Conclusion",
        "",
        "Add observations from actual Spark UI metrics."
    ]

    report = (
        RESULTS_DIR / "performance_results.md"
    )

    report.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    print("Report:", report)


# ========================================
# 12. Main
# ========================================

def main():
    RESULTS_DIR.mkdir(
        parents=True, exist_ok=True
    )

    benchmarks = []
    outputs = []

    for run_number in range(1, RUNS + 1):
        if run_number % 2 == 1:
            sequence = [
                "baseline",
                "optimized"
            ]
        else:
            sequence = [
                "optimized",
                "baseline"
            ]

        for name in sequence:
            benchmark, output_metrics = (
                run_pipeline(
                    name,
                    run_number
                )
            )

            benchmarks.append(benchmark)
            outputs.extend(output_metrics)

    # Verify correctness before reporting success.
    validate_results()

    spark_metrics = parse_event_logs()

    write_csv(
        RESULTS_DIR /
        "performance_benchmark.csv",
        benchmarks
    )

    write_csv(
        RESULTS_DIR /
        "performance_metrics.csv",
        spark_metrics
    )

    write_report(
        benchmarks,
        outputs,
        spark_metrics
    )

    print("\nALL BENCHMARKS COMPLETED")


if __name__ == "__main__":
    main()
