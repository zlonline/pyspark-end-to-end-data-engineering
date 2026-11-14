
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

from week12.config import (
    ROOT_DIR,
    SILVER_DIR,
    GOLD_DIR
)
from week12.ingestion import create_spark


SALES_DETAIL_PATH = SILVER_DIR / "sales_detail"

CUSTOMER_SALES_PATH = GOLD_DIR / "customer_sales"
PRODUCT_SALES_PATH = GOLD_DIR / "product_sales"
DAILY_SALES_PATH = GOLD_DIR / "daily_sales"

RESULTS_DIR = ROOT_DIR / "week12" / "results"


# ========================================
# 1. Read Sales Detail
# ========================================

def read_sales_detail(spark):
    return spark.read.parquet(
        str(SALES_DETAIL_PATH)
    )


# ========================================
# 2. Customer Sales
# ========================================

def aggregate_customer_sales(df):
    result = (
        df.groupBy(
            "customer_id",
            "customer_name",
            "region"
        )
        .agg(
            F.count("*").alias("order_count"),

            F.sum("quantity").alias(
                "total_quantity"
            ),

            F.sum("line_total").alias(
                "total_sales"
            )
        )
        .withColumn(
            "avg_order_value",
            (
                F.col("total_sales") /
                F.col("order_count")
            ).cast(DecimalType(18, 2))
        )
    )

    rank_window = Window.orderBy(
        F.col("total_sales").desc()
    )

    return result.withColumn(
        "sales_rank",
        F.dense_rank().over(rank_window)
    )


# ========================================
# 3. Product Sales
# ========================================

def aggregate_product_sales(df):
    result = (
        df.groupBy(
            "product_id",
            "product_name",
            "category"
        )
        .agg(
            F.count("*").alias("order_count"),

            F.sum("quantity").alias(
                "units_sold"
            ),

            F.sum("line_total").alias(
                "total_sales"
            )
        )
    )

    rank_window = Window.orderBy(
        F.col("total_sales").desc()
    )

    return result.withColumn(
        "sales_rank",
        F.dense_rank().over(rank_window)
    )


# ========================================
# 4. Daily Sales
# ========================================

def aggregate_daily_sales(df):
    result = (
        df.groupBy("order_date")
        .agg(
            F.count("*").alias("order_count"),

            F.sum("quantity").alias(
                "units_sold"
            ),

            F.sum("line_total").alias(
                "daily_revenue"
            )
        )
        .withColumn(
            "sales_year",
            F.year("order_date")
        )
        .withColumn(
            "sales_month",
            F.month("order_date")
        )
    )

    running_window = (
        Window.orderBy("order_date")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    return result.withColumn(
        "running_revenue",
        F.sum("daily_revenue").over(
            running_window
        )
    )


# ========================================
# 5. Save Parquet
# ========================================

def save_parquet(df, path, name):
    start = time.perf_counter()

    df.write.mode("overwrite").parquet(
        str(path)
    )

    elapsed = time.perf_counter() - start

    print(
        f"{name} saved in {elapsed:.2f} sec"
    )

    return elapsed


# ========================================
# 6. Data Validation
# ========================================

def validate_aggregations(
    sales_detail,
    customer_sales,
    product_sales,
    daily_sales
):
    source_metrics = (
        sales_detail.agg(
            F.count("*").alias("orders"),
            F.sum("line_total").alias("revenue")
        ).first()
    )

    source_orders = source_metrics["orders"]
    source_revenue = source_metrics["revenue"]

    for name, df, count_col, revenue_col in [
        (
            "Customer",
            customer_sales,
            "order_count",
            "total_sales"
        ),
        (
            "Product",
            product_sales,
            "order_count",
            "total_sales"
        ),
        (
            "Daily",
            daily_sales,
            "order_count",
            "daily_revenue"
        )
    ]:
        metrics = df.agg(
            F.sum(count_col).alias("orders"),
            F.sum(revenue_col).alias("revenue")
        ).first()

        assert metrics["orders"] == source_orders, (
            f"{name}: order count mismatch"
        )

        assert metrics["revenue"] == source_revenue, (
            f"{name}: revenue mismatch"
        )

        print(f"{name} validation PASSED")

    print(
        "Source Completed Orders:",
        source_orders
    )
    print(
        "Source Total Revenue:",
        source_revenue
    )


# ========================================
# 7. Main
# ========================================

def main():
    spark = create_spark(
        "Week12Day04Aggregation"
    )

    try:
        start = time.perf_counter()

        RESULTS_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        sales_detail = read_sales_detail(spark)

        customer_sales = (
            aggregate_customer_sales(sales_detail)
        )

        product_sales = (
            aggregate_product_sales(sales_detail)
        )

        daily_sales = (
            aggregate_daily_sales(sales_detail)
        )

        print("\n=== CUSTOMER SALES PLAN ===")
        customer_sales.explain("formatted")

        # Save a textual physical plan.
        plan = (
            customer_sales._jdf.queryExecution()
            .executedPlan()
            .toString()
        )

        (
            RESULTS_DIR / "aggregation_plan.txt"
        ).write_text(
            plan,
            encoding="utf-8"
        )

        # Write Gold data.
        save_parquet(
            customer_sales,
            CUSTOMER_SALES_PATH,
            "Customer Sales"
        )

        save_parquet(
            product_sales,
            PRODUCT_SALES_PATH,
            "Product Sales"
        )

        save_parquet(
            daily_sales,
            DAILY_SALES_PATH,
            "Daily Sales"
        )

        # Validate written Gold data.
        customer_saved = spark.read.parquet(
            str(CUSTOMER_SALES_PATH)
        )

        product_saved = spark.read.parquet(
            str(PRODUCT_SALES_PATH)
        )

        daily_saved = spark.read.parquet(
            str(DAILY_SALES_PATH)
        )

        validate_aggregations(
            sales_detail,
            customer_saved,
            product_saved,
            daily_saved
        )

        print("\n=== ROW COUNTS ===")
        print(
            "Customer Sales:",
            customer_saved.count()
        )
        print(
            "Product Sales:",
            product_saved.count()
        )
        print(
            "Daily Sales:",
            daily_saved.count()
        )

        print("\n=== TOP 10 CUSTOMERS ===")
        customer_saved.orderBy(
            "sales_rank", "customer_id"
        ).show(10, truncate=False)

        print("\n=== TOP 10 PRODUCTS ===")
        product_saved.orderBy(
            "sales_rank", "product_id"
        ).show(10, truncate=False)

        print("\n=== DAILY SALES SAMPLE ===")
        daily_saved.orderBy(
            "order_date"
        ).show(10, truncate=False)

        print(
            "\nTotal Time:",
            round(
                time.perf_counter() - start,
                2
            ),
            "seconds"
        )

        print("\nSpark UI: http://localhost:4040")
        input("\nPress Enter to stop Spark...")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
