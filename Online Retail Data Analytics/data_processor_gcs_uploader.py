import os
import time
import argparse
import logging
from datetime import datetime

import pandas as pd
from google.cloud import storage
from google.api_core.exceptions import Conflict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SplitDataByDateRegion:
    def __init__(self, file_path: str, bucket_name: str, blob_list: list) -> None:
        self.file_path = file_path
        self.bucket_name = bucket_name
        self.blob_list = blob_list
        self.storage_client = storage.Client()

    def create_bucket(self) -> None:
        """Creates a new bucket if it doesn't already exist."""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            bucket.storage_class = "STANDARD"
            self.storage_client.create_bucket(bucket, location="US")
            logging.info(f"Bucket {self.bucket_name} created successfully.")
        except Conflict:
            logging.warning(f"Bucket {self.bucket_name} already exists. Skipping creation.")
        except Exception as e:
            logging.error(f"Failed to create bucket {self.bucket_name}: {e}")

    def create_folders_in_bucket(self) -> None:
        """Creates folder-like blobs in the bucket."""
        bucket = self.storage_client.bucket(self.bucket_name)
        for folder in self.blob_list:
            try:
                blob = bucket.blob(f"{folder}/")
                blob.upload_from_string("")  # Create an empty blob to simulate a folder
                logging.info(f"Folder '{folder}' created in bucket '{self.bucket_name}'.")
            except Exception as e:
                logging.error(f"Failed to create folder '{folder}': {e}")

    def upload_blob(self, source_file_name: str, destination_blob_name: str) -> None:
        """Uploads a file to the bucket."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        try:
            blob.upload_from_filename(source_file_name)
            logging.info(f"File '{source_file_name}' uploaded to '{destination_blob_name}'.")
        except Exception as e:
            logging.error(f"Failed to upload file '{source_file_name}': {e}")

    def list_blobs(self) -> None:
        """Lists all blobs in the bucket."""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blobs = bucket.list_blobs()
            logging.info(f"Blobs in bucket '{self.bucket_name}':")
            for blob in blobs:
                logging.info(blob.name)
        except Exception as e:
            logging.error(f"Failed to list blobs: {e}")

    def split_and_upload(self, split_by: str) -> None:
        # sourcery skip: extract-duplicate-method, move-assign-in-block, switch
        """Splits data by the specified column ('Country' or 'Date') and uploads the files."""
        try:
            # Read the entire CSV into memory
            df = pd.read_csv(self.file_path)
            
            
            if split_by == "region":
                count = 0
                unique_values = df["Country"].unique()
                for value in unique_values:
                    region_df = df[df["Country"] == value]
                    file_name = f"Data/{self.blob_list[1]}/{value}-Invoice.csv"
                    region_df.to_csv(file_name, index=False)
                    self.upload_blob(file_name, f"{self.blob_list[0]}/{value}-Invoice.csv")
                    os.remove(file_name)
                    count += 1
                    time.sleep(5)
                    
                    if count == 5:
                        break
            
            elif split_by == "day":
                count = 0
                df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
                df["Date"] = df["InvoiceDate"].dt.date.astype(str)
                unique_dates = df["Date"].unique()
                for date in unique_dates:
                    date_df = df[df["Date"] == date]
                    file_name = f"Data/{self.blob_list[0]}/{date}-Invoice.csv"
                    date_df.to_csv(file_name, index=False)
                    self.upload_blob(file_name, f"{self.blob_list[1]}/{date}-Invoice.csv")
                    os.remove(file_name)
                    count += 1
                    time.sleep(5)
                    
                    if count == 5:
                        break
            
            logging.info(f"Data successfully split by {split_by} and uploaded.")
        
        except Exception as e:
            logging.error(f"Error while splitting and uploading data by {split_by}: {e}")


def main():
    start_time = datetime.now()
    parser = argparse.ArgumentParser(description="Process and upload CSV files to GCS")
    parser.add_argument("--input_file_path", type=str, required=True, help="Path to the input CSV file")
    parser.add_argument("--bucket_name", type=str, required=True, help="GCS bucket name")
    parser.add_argument("--blob_list", type=str, required=True, help="Comma-separated list of folder names")
    args = parser.parse_args()

    csv_file_path = args.input_file_path
    bucket_name = args.bucket_name
    blob_list = args.blob_list.split(",")

    # Ensure local folders for temporary files exist
    os.makedirs("Data", exist_ok=True)
    for folder in blob_list:
        os.makedirs(f"Data/{folder}", exist_ok=True)

    # Instantiate the class and process the file
    processor = SplitDataByDateRegion(file_path=csv_file_path, bucket_name=bucket_name, blob_list=blob_list)
    processor.create_bucket()
    processor.create_folders_in_bucket()
    processor.list_blobs()
    processor.split_and_upload("day")
    # processor.split_and_upload("region")

    logging.info(f"Script completed in {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
