
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from week12.config import (
    SPARK_MASTER,
    ORDERS_PATH,
    CUSTOMERS_PATH,
    PRODUCTS_PATH
)


spark = (
    SparkSession.builder
    .appName("Week12Day01Validation")
    .master(SPARK_MASTER)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    orders = spark.read.parquet(
        str(ORDERS_PATH)
    )

    customers = spark.read.parquet(
        str(CUSTOMERS_PATH)
    )

    products = spark.read.parquet(
        str(PRODUCTS_PATH)
    )

    # ------------------------------------
    # 1. Row Counts
    # ------------------------------------

    order_count = orders.count()
    customer_count = customers.count()
    product_count = products.count()

    print("\n=== ROW COUNTS ===")
    print("Orders:", order_count)
    print("Customers:", customer_count)
    print("Products:", product_count)

    # ------------------------------------
    # 2. Primary Key Uniqueness
    # ------------------------------------

    duplicate_orders = (
        orders.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_orders == 0

    print("Duplicate Order IDs:", duplicate_orders)

    # ------------------------------------
    # 3. Foreign Key Validation
    # ------------------------------------

    missing_customers = (
        orders.select("customer_id").distinct()
        .join(
            customers.select("customer_id"),
            "customer_id",
            "left_anti"
        )
        .count()
    )

    missing_products = (
        orders.select("product_id").distinct()
        .join(
            products.select("product_id"),
            "product_id",
            "left_anti"
        )
        .count()
    )

    assert missing_customers == 0
    assert missing_products == 0

    print("Missing Customers:", missing_customers)
    print("Missing Products:", missing_products)

    # ------------------------------------
    # 4. Business Rules
    # ------------------------------------

    invalid_orders = (
        orders.filter(
            (F.col("quantity") <= 0) |
            (F.col("unit_price") <= 0)
        )
        .count()
    )

    assert invalid_orders == 0

    print("Invalid Orders:", invalid_orders)

    # ------------------------------------
    # 5. Null Validation
    # ------------------------------------

    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "status"
    ]

    null_condition = None

    for column in required_columns:
        condition = F.col(column).isNull()

        if null_condition is None:
            null_condition = condition
        else:
            null_condition = null_condition | condition

    null_rows = orders.filter(
        null_condition
    ).count()

    assert null_rows == 0

    print("Rows with Required Nulls:", null_rows)

    print("\nALL DATA VALIDATION CHECKS PASSED")

finally:
    spark.stop()
