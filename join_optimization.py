import csv
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


RESULTS_DIR = ROOT_DIR / "week12" / "results"

BENCHMARK_DIR = (
    SILVER_DIR / "join_benchmark"
)

RUNS = 3


def create_benchmark_spark(name):
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
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_pipeline(spark, optimized=False):
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
        orders,
        customers,
        products
    )


def save_plan(df, name):
    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    path = (
        RESULTS_DIR /
        f"execution_plan_{name}.txt"
    )

    plan = (
        df._jdf.queryExecution()
        .executedPlan()
        .toString()
    )

    path.write_text(
        plan,
        encoding="utf-8"
    )

    print("Execution Plan:", path)


def run_pipeline(name, run_number):
    optimized = name == "optimized"

    spark = create_benchmark_spark(
        f"Week12_{name}_Run{run_number}"
    )

    try:
        result = build_pipeline(
            spark,
            optimized=optimized
        )

        if run_number == 1:
            print(f"\n=== {name.upper()} PLAN ===")
            result.explain("formatted")
            save_plan(result, name)

        # Each run uses its own output directory.
        output = (
            BENCHMARK_DIR /
            name /
            f"run_{run_number}"
        )

        start = time.perf_counter()

        result.write.mode("overwrite").parquet(
            str(output)
        )

        elapsed = time.perf_counter() - start

        print(
            f"{name} Run {run_number}: "
            f"{elapsed:.3f} sec"
        )

        return elapsed

    finally:
        spark.stop()


def validate_results():
    spark = create_benchmark_spark(
        "Week12JoinResultValidation"
    )

    try:
        baseline = spark.read.parquet(
            str(
                BENCHMARK_DIR /
                "baseline" /
                "run_1"
            )
        )

        optimized = spark.read.parquet(
            str(
                BENCHMARK_DIR /
                "optimized" /
                "run_1"
            )
        )

        # Compare row multiplicity as well as values.
        diff1 = baseline.exceptAll(
            optimized
        ).count()

        diff2 = optimized.exceptAll(
            baseline
        ).count()

        if diff1 != 0 or diff2 != 0:
            raise AssertionError(
                "Baseline and Optimized results differ."
            )

        print("\nRESULT CONSISTENCY PASSED")
        print("Baseline-only rows:", diff1)
        print("Optimized-only rows:", diff2)

    finally:
        spark.stop()


def main():
    results = []

    # Alternate run order to reduce bias.
    for run_number in range(1, RUNS + 1):
        if run_number % 2 == 1:
            sequence = ["baseline", "optimized"]
        else:
            sequence = ["optimized", "baseline"]

        for name in sequence:
            elapsed = run_pipeline(
                name,
                run_number
            )

            results.append({
                "run": run_number,
                "pipeline": name,
                "elapsed_seconds": round(
                    elapsed, 3
                )
            })

    validate_results()

    baseline_times = [
        r["elapsed_seconds"]
        for r in results
        if r["pipeline"] == "baseline"
    ]

    optimized_times = [
        r["elapsed_seconds"]
        for r in results
        if r["pipeline"] == "optimized"
    ]

    baseline_median = statistics.median(
        baseline_times
    )

    optimized_median = statistics.median(
        optimized_times
    )

    improvement = (
        (baseline_median - optimized_median)
        / baseline_median * 100
    )

    speedup = (
        baseline_median / optimized_median
    )

    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    csv_path = (
        RESULTS_DIR / "join_benchmark.csv"
    )

    with csv_path.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "run",
                "pipeline",
                "elapsed_seconds"
            ]
        )

        writer.writeheader()
        writer.writerows(results)

    print("\n=== BENCHMARK SUMMARY ===")
    print(
        "Baseline Median:",
        round(baseline_median, 3)
    )
    print(
        "Optimized Median:",
        round(optimized_median, 3)
    )
    print(
        "Speedup:",
        round(speedup, 2),
        "x"
    )
    print(
        "Improvement:",
        round(improvement, 2),
        "%"
    )

    print("\nCSV:", csv_path)


if __name__ == "__main__":
    main()
