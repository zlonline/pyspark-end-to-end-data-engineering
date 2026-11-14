
import argparse
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from week12.config import (
    ORDER_COUNT,
    CUSTOMER_COUNT,
    PRODUCT_COUNT,
    DATA_SEED,
    START_DATE,
    SPARK_MASTER,
    SHUFFLE_PARTITIONS,
    GENERATE_PARTITIONS,
    APP_NAME,
    ORDERS_PATH,
    CUSTOMERS_PATH,
    PRODUCTS_PATH,
    create_directories
)


# ========================================
# 1. Logging
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ========================================
# 2. Spark Session
# ========================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER)
        .config(
            "spark.sql.shuffle.partitions",
            str(SHUFFLE_PARTITIONS)
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ========================================
# 3. Generate Customers
# ========================================

def generate_customers(spark, customer_count):
    logger.info("Generating Customers...")

    customers = (
        spark.range(
            1,
            customer_count + 1,
            1,
            numPartitions=GENERATE_PARTITIONS
        )
        .select(
            F.col("id")
            .cast("long")
            .alias("customer_id"),

            F.concat(
                F.lit("Customer_"),
                F.col("id")
            ).alias("customer_name"),

            F.when(
                F.col("id") % 4 == 0, "Ontario"
            ).when(
                F.col("id") % 4 == 1, "Quebec"
            ).when(
                F.col("id") % 4 == 2, "Alberta"
            ).otherwise(
                "British Columbia"
            ).alias("region"),

            F.date_add(
                F.to_date(F.lit(START_DATE)),
                (F.col("id") % 365).cast("int")
            ).alias("signup_date")
        )
    )

    return customers


# ========================================
# 4. Generate Products
# ========================================

def generate_products(spark, product_count):
    logger.info("Generating Products...")

    products = (
        spark.range(
            1,
            product_count + 1,
            1,
            numPartitions=GENERATE_PARTITIONS
        )
        .select(
            F.col("id")
            .cast("long")
            .alias("product_id"),

            F.concat(
                F.lit("Product_"),
                F.col("id")
            ).alias("product_name"),

            F.when(
                F.col("id") % 4 == 0, "Electronics"
            ).when(
                F.col("id") % 4 == 1, "Clothing"
            ).when(
                F.col("id") % 4 == 2, "Home"
            ).otherwise(
                "Sports"
            ).alias("category"),

            (
                F.col("id") % 500 + 10
            ).cast(
                DecimalType(12, 2)
            ).alias("list_price")
        )
    )

    return products


# ========================================
# 5. Generate Orders
# ========================================

def generate_orders(
    spark,
    order_count,
    customer_count,
    product_count
):
    logger.info("Generating Orders...")

    # Range ID: 0 to order_count - 1
    base = spark.range(
        0,
        order_count,
        1,
        numPartitions=GENERATE_PARTITIONS
    )

    # Deterministic pseudo-random-looking IDs.
    # Multiplier 37 is coprime with 50,000.
    # Multiplier 17 is coprime with 5,000.

    orders = (
        base.select(
            (F.col("id") + 1)
            .cast("long")
            .alias("order_id"),

            (
                (
                    F.col("id") * 37 + DATA_SEED
                ) % customer_count + 1
            ).cast("long").alias("customer_id"),

            (
                (
                    F.col("id") * 17 + DATA_SEED
                ) % product_count + 1
            ).cast("long").alias("product_id"),

            F.date_add(
                F.to_date(F.lit(START_DATE)),
                (F.col("id") % 730).cast("int")
            ).alias("order_date"),

            (
                F.col("id") % 5 + 1
            ).cast("int").alias("quantity"),

            (
                F.col("id") % 200 + 20
            ).cast(
                DecimalType(12, 2)
            ).alias("unit_price"),

            F.when(
                F.col("id") % 10 == 0,
                "Cancelled"
            ).when(
                F.col("id") % 10 == 1,
                "Pending"
            ).otherwise(
                "Completed"
            ).alias("status")
        )
    )

    return orders


# ========================================
# 6. Write Parquet
# ========================================

def write_dataset(df, path, name):
    start = time.perf_counter()

    logger.info(
        "Writing %s to %s",
        name,
        path
    )

    df.write.mode("overwrite").parquet(
        str(path)
    )

    elapsed = time.perf_counter() - start

    logger.info(
        "%s written in %.2f seconds",
        name,
        elapsed
    )

    return elapsed


# ========================================
# 7. Main Pipeline
# ========================================

def main(
    order_count,
    customer_count,
    product_count
):
    if min(
        order_count,
        customer_count,
        product_count
    ) <= 0:
        raise ValueError(
            "All dataset counts must be positive."
        )

    create_directories()

    spark = create_spark()

    try:
        start_total = time.perf_counter()

        customers = generate_customers(
            spark,
            customer_count
        )

        products = generate_products(
            spark,
            product_count
        )

        orders = generate_orders(
            spark,
            order_count,
            customer_count,
            product_count
        )

        # --------------------------------
        # Display Schema
        # --------------------------------

        print("\n=== ORDERS SCHEMA ===")
        orders.printSchema()

        print("\n=== CUSTOMERS SCHEMA ===")
        customers.printSchema()

        print("\n=== PRODUCTS SCHEMA ===")
        products.printSchema()

        # --------------------------------
        # Partition Information
        # --------------------------------

        print("\n=== PARTITION COUNTS ===")

        print(
            "Orders:",
            orders.rdd.getNumPartitions()
        )

        print(
            "Customers:",
            customers.rdd.getNumPartitions()
        )

        print(
            "Products:",
            products.rdd.getNumPartitions()
        )

        # --------------------------------
        # Write Bronze Data
        # --------------------------------

        write_dataset(
            customers,
            CUSTOMERS_PATH,
            "Customers"
        )

        write_dataset(
            products,
            PRODUCTS_PATH,
            "Products"
        )

        write_dataset(
            orders,
            ORDERS_PATH,
            "Orders"
        )

        # --------------------------------
        # Read Back + Validation
        # --------------------------------

        print("\n=== VALIDATE ROW COUNTS ===")

        actual_orders = spark.read.parquet(
            str(ORDERS_PATH)
        ).count()

        actual_customers = spark.read.parquet(
            str(CUSTOMERS_PATH)
        ).count()

        actual_products = spark.read.parquet(
            str(PRODUCTS_PATH)
        ).count()

        assert actual_orders == order_count
        assert actual_customers == customer_count
        assert actual_products == product_count

        print("Orders:", actual_orders)
        print("Customers:", actual_customers)
        print("Products:", actual_products)

        print("\n=== SAMPLE ORDERS ===")
        spark.read.parquet(
            str(ORDERS_PATH)
        ).show(10, truncate=False)

        total_time = (
            time.perf_counter() - start_total
        )

        print("\n=== GENERATION COMPLETED ===")
        print(
            "Total Time:",
            round(total_time, 2),
            "seconds"
        )

        print("\nSpark UI: http://localhost:4040")

        input(
            "\nPress Enter to stop Spark..."
        )

    finally:
        spark.stop()


# ========================================
# 8. Entry Point
# ========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--orders",
        type=int,
        default=ORDER_COUNT
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=CUSTOMER_COUNT
    )

    parser.add_argument(
        "--products",
        type=int,
        default=PRODUCT_COUNT
    )

    args = parser.parse_args()

    main(
        order_count=args.orders,
        customer_count=args.customers,
        product_count=args.products
    )
