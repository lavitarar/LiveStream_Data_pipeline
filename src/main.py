from pyspark.sql import SparkSession
from extract import extract_new_data
from transform import transform_data
from s3_upload import upload_to_s3

def create_spark_session():
    session = SparkSession.builder.appName("Postgres-to-s3").getOrCreate()

    return session

def main():
    print("-" * 60)
    print("Postgres → PYSPARK → S3")
    print("-" * 60)

    spark = create_spark_session()

    try:
        columns , rows = extract_new_data()

        if not rows:
            print("Data Not Found!")
            return

        transformed_df=transform_data(spark,columns , rows)

        upload_to_s3(transformed_df)
        print("--" * 60)
        print("Pipeline Completed Sucessfully")
        print("--" * 60)

    except Exception as e:
        print(f"Pipeline Failed : {e}")

    finally:
        spark.stop()
        print("Spark Stopped Sucessfully!")

if __name__ == "__main__":
    main()