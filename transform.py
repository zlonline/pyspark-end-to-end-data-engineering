
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from week12.config import SILVER_DIR
from week12.ingestion import create_spark


ORDERS_CLEAN = SILVER_DIR / "orders_clean"
CUSTOMERS_CLEAN = SILVER_DIR / "customers_clean"
PRODUCTS_CLEAN = SILVER_DIR / "products_clean"

SALES_DETAIL = SILVER_DIR / "sales_detail"


def read_clean_data(spark):
    orders = spark.read.parquet(
        str(ORDERS_CLEAN)
    )

    customers = spark.read.parquet(
        str(CUSTOMERS_CLEAN)
    )

    products = spark.read.parquet(
        str(PRODUCTS_CLEAN)
    )

    return orders, customers, products


def transform_orders(orders):
    return (
        orders
        .filter(
            F.col("status") == "Completed"
        )
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price"
        )
        .withColumn(
            "line_total",
            (
                F.col("quantity") *
                F.col("unit_price")
            ).cast(DecimalType(18, 2))
        )
    )


def select_customers(customers):
    return customers.select(
        "customer_id",
        "customer_name",
        "region"
    )


def select_products(products):
    return products.select(
        "product_id",
        "product_name",
        "category"
    )


def build_sales_detail(
    orders,
    customers,
    products
):
    result = (
        orders
        .join(
            customers,
            "customer_id",
            "inner"
        )
        .join(
            products,
            "product_id",
            "inner"
        )
    )

    return result.select(
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "region",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "line_total"
    )


def main():
    spark = create_spark(
        "Week12Day03Transformation"
    )

    try:
        orders, customers, products = (
            read_clean_data(spark)
        )

        orders = transform_orders(orders)
        customers = select_customers(customers)
        products = select_products(products)

        result = build_sales_detail(
            orders,
            customers,
            products
        )

        print("\n=== SALES DETAIL SCHEMA ===")
        result.printSchema()

        print("\n=== EXECUTION PLAN ===")
        result.explain("formatted")

        result.write.mode("overwrite").parquet(
            str(SALES_DETAIL)
        )

        print("\n=== OUTPUT VALIDATION ===")
        saved = spark.read.parquet(
            str(SALES_DETAIL)
        )

        print("Sales Detail Rows:", saved.count())

        saved.show(10, truncate=False)

        print("\nOutput:", SALES_DETAIL)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
