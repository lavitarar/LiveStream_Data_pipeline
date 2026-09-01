import os
import shutil
import tempfile
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def upload_to_s3(df, total_records):
    bucket = os.getenv("S3_BUCKET")
    prefix=os.getenv("S3_PREFIX","source_file/")
    batch_size = 200
    # total_records = df.rdd.getNumPartitions()

    if total_records ==0:
        print("No Records Found!")
        return []

    print(f"Records to upload: {total_records}")
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    processed_ids =[row['order_id'] for row in df.select("order_id").collect()]
    temp_directory = tempfile.mkdtemp()

    try:
        batch_number = 1

        for start in range(0,total_records,batch_size):
            end = min(start+batch_size,total_records)
            batch_df = (df.orderBy("order_id").limit(end))

            if start > 0:
                previous_df = (df.orderBy("order_id").limit(start))
                batch_df = batch_df.subtract(previous_df)

            batch_count = batch_df.count()

            if batch_count ==0:
                continue

            batch_path = os.path.join(temp_directory,f"batch_{batch_number}")

            (batch_df.coalesce(1).write.mode("overwrite")
                .option("header","true").csv(batch_path))

            csv_file = None

            for root , dirs , files in os.walk(batch_path):
                for file in files:
                    if file.startswith("part-") and file.endswith(".csv"):
                        csv_file = os.path.join(root,file)
                        break
                if csv_file is not None:
                    break

            if csv_file is None:
                raise Exception("CSV File is Not Generated!")

            #S3 Filaname
            s3_key = f"{prefix}part-00000_orders_{batch_number}.csv"
            s3=boto3.client("s3",region_name=os.getenv("AWS_REGION"))

            print(type(csv_file))
            print(type(bucket))
            print(type(s3_key))   

            s3.upload_file(csv_file,bucket,s3_key)
            print(f"Uploaded: s3://{bucket}/{s3_key}")
            print(f"Rows in batch: {batch_count}")

            batch_number +=1 

    finally:
        shutil.rmtree(temp_directory,ignore_errors=True)

    return processed_ids




            




    


