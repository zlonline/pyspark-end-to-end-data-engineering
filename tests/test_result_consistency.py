
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from week12.config import (
    ROOT_DIR,
    SILVER_DIR,
    GOLD_DIR,
    SPARK_MASTER
)


SALES_DETAIL_PATH = (
    SILVER_DIR / "sales_detail"
)

CUSTOMER_SALES_PATH = (
    GOLD_DIR / "customer_sales"
)

PRODUCT_SALES_PATH = (
    GOLD_DIR / "product_sales"
)

DAILY_SALES_PATH = (
    GOLD_DIR / "daily_sales"
)

PERFORMANCE_ROOT = (
    GOLD_DIR / "performance"
)


# ========================================
# 1. Spark Fixture
# ========================================

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("Week12ResultConsistencyTest")
        .master(SPARK_MASTER)
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


# ========================================
# 2. Check Paths
# ========================================

def test_required_outputs_exist():
    paths = [
        SALES_DETAIL_PATH,
        CUSTOMER_SALES_PATH,
        PRODUCT_SALES_PATH,
        DAILY_SALES_PATH
    ]

    for path in paths:
        assert path.exists(), (
            f"Missing output: {path}"
        )

        assert list(path.rglob("*.parquet")), (
            f"No Parquet files found: {path}"
        )


# ========================================
# 3. Check Non-empty Datasets
# ========================================

def test_gold_datasets_not_empty(spark):
    for path in [
        CUSTOMER_SALES_PATH,
        PRODUCT_SALES_PATH,
        DAILY_SALES_PATH
    ]:
        df = spark.read.parquet(str(path))

        assert df.limit(1).count() > 0, (
            f"Empty dataset: {path}"
        )


# ========================================
# 4. Revenue Reconciliation
# ========================================

def test_revenue_reconciliation(spark):
    sales = spark.read.parquet(
        str(SALES_DETAIL_PATH)
    )

    source_revenue = sales.agg(
        F.sum("line_total").alias("revenue")
    ).first()["revenue"]

    datasets = [
        (
            CUSTOMER_SALES_PATH,
            "total_sales"
        ),
        (
            PRODUCT_SALES_PATH,
            "total_sales"
        ),
        (
            DAILY_SALES_PATH,
            "daily_revenue"
        )
    ]

    for path, revenue_col in datasets:
        df = spark.read.parquet(str(path))

        gold_revenue = df.agg(
            F.sum(revenue_col).alias("revenue")
        ).first()["revenue"]

        assert gold_revenue == source_revenue, (
            f"Revenue mismatch: {path}"
        )


# ========================================
# 5. Order Count Reconciliation
# ========================================

def test_order_count_reconciliation(spark):
    sales = spark.read.parquet(
        str(SALES_DETAIL_PATH)
    )

    source_count = sales.count()

    for path in [
        CUSTOMER_SALES_PATH,
        PRODUCT_SALES_PATH,
        DAILY_SALES_PATH
    ]:
        df = spark.read.parquet(str(path))

        gold_count = df.agg(
            F.sum("order_count").alias("orders")
        ).first()["orders"]

        assert gold_count == source_count, (
            f"Order count mismatch: {path}"
        )


# ========================================
# 6. Baseline vs Optimized
# ========================================

@pytest.mark.parametrize(
    "dataset",
    [
        "customer_sales",
        "product_sales",
        "daily_sales"
    ]
)
def test_baseline_optimized_consistency(
    spark,
    dataset
):
    baseline_path = (
        PERFORMANCE_ROOT /
        "baseline" /
        "run_1" /
        dataset
    )

    optimized_path = (
        PERFORMANCE_ROOT /
        "optimized" /
        "run_1" /
        dataset
    )

    if not (
        baseline_path.exists()
        and optimized_path.exists()
    ):
        pytest.skip(
            "Performance benchmark outputs "
            "are not available."
        )

    baseline = spark.read.parquet(
        str(baseline_path)
    )

    optimized = spark.read.parquet(
        str(optimized_path)
    )

    assert baseline.schema == optimized.schema, (
        f"Schema mismatch: {dataset}"
    )

    diff_1 = baseline.exceptAll(
        optimized
    ).count()

    diff_2 = optimized.exceptAll(
        baseline
    ).count()

    assert diff_1 == 0
    assert diff_2 == 0
