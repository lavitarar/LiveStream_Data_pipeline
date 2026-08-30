from pyspark.sql import DataFrame
from pyspark.sql.functions import *


# def transform_data():

# ----------------------------------------------------------------------------------------------


def transform_data(
    spark,
    columns,
    rows
) -> DataFrame:

    # Create Spark DataFrame
    df = spark.createDataFrame(
        rows,
        columns
    )

    print(f"Before transformation: {df.count()} records")

    # --------------------------------------------------
    # 1. Remove duplicate order IDs
    # --------------------------------------------------

    df = df.dropDuplicates(["order_id"])

    # --------------------------------------------------
    # 2. Convert data types
    # --------------------------------------------------

    df = df.withColumn(
        "order_id",
        col("order_id").cast("long")
    )

    df = df.withColumn(
        "customer_id",
        col("customer_id").cast("long")
    )

    df = df.withColumn(
        "product_id",
        col("product_id").cast("long")
    )

    df = df.withColumn(
        "quantity",
        col("quantity").cast("integer")
    )

    df = df.withColumn(
        "price",
        col("price").cast("double")
    )

    # --------------------------------------------------
    # 3. Handle NULL values
    # --------------------------------------------------

    df = df.withColumn(
        "quantity",
        coalesce(
            col("quantity"),
            lit(0)
        )
    )

    df = df.withColumn(
        "price",
        coalesce(
            col("price"),
            lit(0.0)
        )
    )

    # --------------------------------------------------
    # 4. Clean order date
    # --------------------------------------------------

    df = df.withColumn(
        "order_date",
        to_date(col("order_date"))
    )

    # --------------------------------------------------
    # 5. Create total amount
    # --------------------------------------------------

    df = df.withColumn(
        "total_amount",
        round(
            col("quantity") * col("price"),
            2
        )
    )

    # --------------------------------------------------
    # 6. Select final columns
    # --------------------------------------------------

    df = df.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "order_date",
        "total_amount"
    )

    print(
        f"After transformation: {df.count()} records"
    )

    return df