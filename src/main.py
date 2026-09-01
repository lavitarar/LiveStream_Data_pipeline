import sys
import os

PYTHON_PATH = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

from pyspark.sql import SparkSession
from extract import extract_new_data
from transform import transform_data
from s3_upload import upload_to_s3


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Postgres-to-s3")
        .master("local[1]")
        .config("spark.pyspark.python", PYTHON_PATH)
        .config("spark.pyspark.driver.python", PYTHON_PATH)
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )


def main():
    print("-" * 60)
    print("Postgres -> PYSPARK -> S3")
    print("-" * 60)

    spark = create_spark_session()

    try:
        columns, rows = extract_new_data()

        if not rows:
            print("Data Not Found!")
            return

        print("Starting transformation...")

        transformed_df = transform_data(spark, columns, rows)

        print("Transformation completed!")

        print("Starting S3 upload...")

        upload_to_s3(transformed_df)

        print("S3 upload completed!")

        print("--" * 60)
        print("Pipeline Completed Successfully")
        print("--" * 60)

    except Exception as e:
        import traceback
        print(f"Pipeline Failed : {e}")
        traceback.print_exc()

    finally:
        print("Stopping Spark")
        spark.stop()
        print("Spark Stopped Successfully!")


if __name__ == "__main__":
    main()