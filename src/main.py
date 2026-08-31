import os
from pyspark.sql import SparkSession
from extract import extract_new_data
from transform import transform_data
from s3_upload import upload_to_s3

from db_connection import get_db_connection


def create_spark_session():

    return (
        SparkSession.builder
        .appName("PostgreSQL-To-S3")
        .getOrCreate()
    )


def mark_as_processed(order_ids):

    if not order_ids:
        return

    connection = get_db_connection()

    cursor = connection.cursor()

    query = """
        INSERT INTO etl_processed_orders (
            order_id
        )
        VALUES (%s)
        ON CONFLICT (order_id)
        DO NOTHING;
    """

    for order_id in order_ids:

        cursor.execute(
            query,
            (order_id,)
        )

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"Marked {len(order_ids)} "
        f"records as processed."
    )


def main():

    print("=" * 60)
    print("POSTGRESQL → PYSPARK → S3")
    print("=" * 60)

    # -----------------------------------------
    # 1. Create Spark session
    # -----------------------------------------

    spark = create_spark_session()

    try:

        # -----------------------------------------
        # 2. Extract new data
        # -----------------------------------------

        columns, rows = extract_new_data()

        if not rows:

            print("No new data found.")

            return

        # -----------------------------------------
        # 3. Transformation
        # -----------------------------------------

        transformed_df = transform_data(
            spark,
            columns,
            rows
        )

        # -----------------------------------------
        # 4. Upload to S3
        # -----------------------------------------

        processed_ids = upload_to_s3(
            transformed_df
        )

        # -----------------------------------------
        # 5. Mark records as processed
        # -----------------------------------------

        mark_as_processed(
            processed_ids
        )

        print("=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception as e:

        print(
            "Pipeline failed:"
        )

        print(e)

        raise

    finally:

        spark.stop()


if __name__ == "__main__":

    main()