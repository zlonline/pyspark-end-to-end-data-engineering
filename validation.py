
from pyspark.sql import functions as F

from week12.config import SILVER_DIR
from week12.ingestion import create_spark


def count_null_rows(df, columns):
    condition = None

    for name in columns:
        current = F.col(name).isNull()

        condition = (
            current if condition is None
            else condition | current
        )

    return df.filter(condition).count()


def duplicate_key_count(df, key):
    return (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def validate_dataset(
    spark,
    name,
    key,
    required_columns
):
    path = SILVER_DIR / f"{name}_clean"

    df = spark.read.parquet(str(path))

    row_count = df.count()

    null_count = count_null_rows(
        df, required_columns
    )

    duplicate_count = duplicate_key_count(
        df, key
    )

    print(f"\n=== {name.upper()} ===")
    print("Row Count:", row_count)
    print("Null Rows:", null_count)
    print("Duplicate Keys:", duplicate_count)

    assert row_count > 0
    assert null_count == 0
    assert duplicate_count == 0

    return df


def main():
    spark = create_spark(
        "Week12Day02Validation"
    )

    try:
        orders = validate_dataset(
            spark,
            "orders",
            "order_id",
            [
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "status"
            ]
        )

        customers = validate_dataset(
            spark,
            "customers",
            "customer_id",
            [
                "customer_id",
                "customer_name"
            ]
        )

        products = validate_dataset(
            spark,
            "products",
            "product_id",
            [
                "product_id",
                "product_name",
                "list_price"
            ]
        )

        # Business rule validation.
        invalid_orders = orders.filter(
            (F.col("quantity") <= 0) |
            (F.col("unit_price") <= 0) |
            (~F.col("status").isin(
                "Completed",
                "Pending",
                "Cancelled"
            ))
        ).count()

        invalid_products = products.filter(
            F.col("list_price") <= 0
        ).count()

        assert invalid_orders == 0
        assert invalid_products == 0

        # Foreign key validation.
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

        print("\nMissing Customers:", missing_customers)
        print("Missing Products:", missing_products)

        print("\nALL SILVER VALIDATIONS PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
