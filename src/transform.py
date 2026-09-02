from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_data(spark, columns, rows) -> DataFrame:

    df = spark.createDataFrame(rows, columns)

    print("DataFrame created successfully!")
    print("Columns:", df.columns)

    df = df.withColumn("order_id", col("order_id").cast("integer"))
    df = df.withColumn("order_date", col("order_date").cast("string"))
    df = df.withColumn("order_customer_id", col("order_customer_id").cast("integer"))
    df = df.withColumn("order_status", col("order_status").cast("string"))

    df = df.dropna()
    df = df.dropDuplicates(["order_id"])
    df = df.select("order_id","order_date","order_customer_id","order_status")

    return df
