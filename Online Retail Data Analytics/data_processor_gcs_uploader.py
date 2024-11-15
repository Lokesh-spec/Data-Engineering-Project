import os
import time
import argparse

import pandas as pd

from google.cloud import storage


class SplitDataByDateRegion:
    def __init__(self, file_path):
        self.file_path = file_path
        
    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to upload is aborted if the object's
        # generation number does not match your precondition. For a destination
        # object that does not yet exist, set the if_generation_match precondition to 0.
        # If the destination object already exists in your bucket, set instead a
        # generation-match precondition using its generation number.
        generation_match_precondition = 0

        blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )

    def split_by_day(self):
        df = pd.read_csv(self.file_path)
        
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
        
        df["Date"] = df["InvoiceDate"].dt.date.astype("str")
        
        df["Time"] = df["InvoiceDate"].dt.time.astype("str")
        
        date_list = df["Date"].unique()
        
        for date in date_list:
            print(date)
            date_df = df[df["Date"] == date]
            
            date_df.to_csv(f"Data/{date}-Invoice.csv")
            
            self.upload_blob("online_retail_invoice", f"Data/{date}-Invoice.csv", f"online_retail_invoice_day_wise/{date}-Invoice.csv")   
            
            time.sleep(2)
        
        # df[["Date", "Time"]] = df["InvoiceDate"].str.split(" ")
        
        # print(df[["Date", "Time"]])



if __name__ == "__main__":
    
    import os
    
    try:
        os.mkdir("Data")
    except FileExistsError:
        print("File Already Exists!!!")
    # Initialize the parser
    parser = argparse.ArgumentParser(description="CSV file path argument example")

    # Add arguments
    parser.add_argument("--input_file_path", type=str, required=True, help="Path to the CSV file")

    # Parse the arguments
    args = parser.parse_args()

    # Access the file path argument
    csv_file_path = args.input_file_path
    
    split_data_by_date_region = SplitDataByDateRegion(file_path=csv_file_path)
    
    split_data_by_date_region.split_by_day()