import os
import shutil
import tempfile

import boto3

from datetime import datetime


def upload_to_s3(df):

    bucket = os.getenv("S3_BUCKET")

    prefix = os.getenv(
        "S3_PREFIX",
        "source_file/"
    )

    batch_size = 200

    total_records = df.count()

    if total_records == 0:

        print("No records to upload.")

        return []

    print(
        f"Records to upload: {total_records}"
    )

    # Create unique run timestamp
    run_timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    # Collect order IDs for control table
    processed_ids = [
        row["order_id"]
        for row in df.select("order_id").collect()
    ]

    # Create temporary directory
    temp_directory = tempfile.mkdtemp()

    try:

        batch_number = 1

        for start in range(
            0,
            total_records,
            batch_size
        ):

            end = min(
                start + batch_size,
                total_records
            )

            # Get batch
            batch_df = (
                df
                .orderBy("order_id")
                .limit(end)
            )

            if start > 0:

                previous_df = (
                    df
                    .orderBy("order_id")
                    .limit(start)
                )

                batch_df = batch_df.subtract(
                    previous_df
                )

            batch_count = batch_df.count()

            if batch_count == 0:
                continue

            # Temporary batch directory
            batch_path = os.path.join(
                temp_directory,
                f"batch_{batch_number}"
            )

            # Write CSV
            (
                batch_df
                .coalesce(1)
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv(batch_path)
            )

            # Find Spark generated CSV
            csv_file = None

            for root, dirs, files in os.walk(
                batch_path
            ):

                for file in files:

                    if (
                        file.startswith("part-")
                        and file.endswith(".csv")
                    ):

                        csv_file = os.path.join(
                            root,
                            file
                        )

                        break

            if csv_file is None:

                raise Exception(
                    "CSV file was not generated."
                )

            # Unique S3 filename
            s3_key = (
                f"{prefix}"
                f"orders_"
                f"{run_timestamp}_"
                f"batch_{batch_number:04d}.csv"
            )

            # Upload
            s3 = boto3.client(
                "s3",
                region_name=os.getenv(
                    "AWS_REGION"
                )
            )

            s3.upload_file(
                csv_file,
                bucket,
                s3_key
            )

            print(
                f"Uploaded: "
                f"s3://{bucket}/{s3_key}"
            )

            print(
                f"Rows in batch: {batch_count}"
            )

            batch_number += 1

    finally:

        shutil.rmtree(
            temp_directory,
            ignore_errors=True
        )

    return processed_ids