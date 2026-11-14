
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType,
    StringType, DateType, DecimalType
)

from week12.config import (
    SPARK_MASTER,
    SHUFFLE_PARTITIONS,
    ORDERS_PATH,
    CUSTOMERS_PATH,
    PRODUCTS_PATH
)


ORDERS_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField(
        "unit_price", DecimalType(12, 2), True
    ),
    StructField("status", StringType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", LongType(), True),
    StructField("customer_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", DateType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField(
        "list_price", DecimalType(12, 2), True
    ),
])


def create_spark(app_name="Week12DataCleaning"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config(
            "spark.sql.shuffle.partitions",
            str(SHUFFLE_PARTITIONS)
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_schema(df, expected_schema, name):
    actual = {
        f.name: f.dataType
        for f in df.schema.fields
    }

    expected = {
        f.name: f.dataType
        for f in expected_schema.fields
    }

    if actual != expected:
        raise ValueError(
            f"{name} schema mismatch.\n"
            f"Expected: {expected}\n"
            f"Actual: {actual}"
        )

    print(f"{name} schema validation PASSED")


def read_bronze(spark):
    datasets = [
        ("Orders", ORDERS_PATH, ORDERS_SCHEMA),
        ("Customers", CUSTOMERS_PATH, CUSTOMERS_SCHEMA),
        ("Products", PRODUCTS_PATH, PRODUCTS_SCHEMA)
    ]

    result = {}

    for name, path, schema in datasets:
        df = spark.read.parquet(str(path))

        validate_schema(df, schema, name)

        result[name.lower()] = df

    return result


if __name__ == "__main__":
    spark = create_spark("Day02Ingestion")

    try:
        datasets = read_bronze(spark)

        for name, df in datasets.items():
            print(f"\n=== {name.upper()} ===")
            df.printSchema()
            df.show(5, truncate=False)

    finally:
        spark.stop()
