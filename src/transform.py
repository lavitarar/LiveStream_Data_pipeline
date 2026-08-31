from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform_data(spark, columns, rows) -> DataFrame:
    df = spark.createDataFrame(rows, columns)
    print(f"Before transformation: {df.count()} records")

    # Convert Data Types
    df = df.withColumn("order_id",col("order_id").cast("integer"))
    df = df.withColumn("order_date",col("order_date").cast("string"))
    df = df.withColumn("order_customer_id",col("order_customer_id").cast("integer"))
    df = df.withColumn("order_status",col("order_status").cast("string"))

    # Drop NULL values
    df = df.dropna()

    # Remove duplicate order_id
    df = df.dropDuplicates(["order_id"])

    # Select final columns
    df = df.select("order_id","order_date","order_customer_id","order_status")
    print(f"After transformation: {df.count()} records")

    return df


