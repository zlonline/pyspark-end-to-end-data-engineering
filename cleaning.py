
import logging
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from week12.config import SILVER_DIR
from week12.ingestion import (
    create_spark,
    read_bronze
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def ensure_unique_key(df, key, name):
    duplicates = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            f"{name}: conflicting duplicate {key}"
        )


def clean_orders(df):
    # Remove exact duplicate records only.
    df = df.dropDuplicates()

    # Reject conflicting duplicate order IDs.
    ensure_unique_key(df, "order_id", "Orders")

    # Required fields cannot be null.
    df = df.dropna(
        subset=[
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price"
        ]
    )

    # Normalize status.
    df = df.withColumn(
        "status",
        F.when(
            F.col("status").isNull(),
            F.lit("Unknown")
        ).otherwise(F.trim(F.col("status")))
    )

    # Validate positive values and valid status.
    df = df.filter(
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0) &
        F.col("status").isin(
            "Completed",
            "Pending",
            "Cancelled"
        )
    )

    # Explicit business data types.
    df = (
        df
        .withColumn(
            "quantity",
            F.col("quantity").cast("int")
        )
        .withColumn(
            "unit_price",
            F.col("unit_price").cast(
                DecimalType(12, 2)
            )
        )
    )

    return df


def clean_customers(df):
    df = df.dropDuplicates()

    ensure_unique_key(
        df, "customer_id", "Customers"
    )

    df = df.dropna(
        subset=["customer_id", "customer_name"]
    )

    df = (
        df
        .withColumn(
            "customer_name",
            F.trim(F.col("customer_name"))
        )
        .fillna({"region": "Unknown"})
    )

    return df


def clean_products(df):
    df = df.dropDuplicates()

    ensure_unique_key(
        df, "product_id", "Products"
    )

    df = df.dropna(
        subset=[
            "product_id",
            "product_name",
            "list_price"
        ]
    )

    df = (
        df
        .withColumn(
            "product_name",
            F.trim(F.col("product_name"))
        )
        .fillna({"category": "Unknown"})
        .filter(F.col("list_price") > 0)
    )

    return df


def main():
    spark = create_spark(
        "Week12Day02Cleaning"
    )

    try:
        start = time.perf_counter()

        bronze = read_bronze(spark)

        cleaning_functions = {
            "orders": clean_orders,
            "customers": clean_customers,
            "products": clean_products
        }

        for name, clean_func in cleaning_functions.items():
            source = bronze[name]

            before_count = source.count()

            cleaned = clean_func(source)

            after_count = cleaned.count()

            rejected_count = (
                before_count - after_count
            )

            output = (
                SILVER_DIR / f"{name}_clean"
            )

            cleaned.write.mode(
                "overwrite"
            ).parquet(str(output))

            logger.info(
                "%s: before=%d, after=%d, "
                "removed=%d",
                name,
                before_count,
                after_count,
                rejected_count
            )

            print(
                f"\n{name.upper()} CLEANED"
            )
            print("Before:", before_count)
            print("After:", after_count)
            print("Removed:", rejected_count)
            print("Output:", output)

        print(
            "\nCleaning completed in",
            round(
                time.perf_counter() - start, 2
            ),
            "seconds"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
