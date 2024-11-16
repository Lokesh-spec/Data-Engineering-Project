# **Script Workflow: Splitting and Uploading CSV Data to Google Cloud Storage**

## **Overview**

This Python script processes a CSV file by splitting it based on region and date, then uploads the split files to specified folders in a Google Cloud Storage (GCS) bucket. The script also handles bucket creation, folder simulation in GCS, and file uploads.

---

## **1. Script Objectives**

1. **Bucket Management**:
   - Create a new GCS bucket if it doesn’t already exist.
   - Simulate folder structure in the GCS bucket.

2. **Data Processing**:
   - Split the CSV file based on:
     - **Region** (e.g., `Country` column).
     - **Date** (e.g., `InvoiceDate` column).
   - Save the split files locally in a structured directory.

3. **Data Upload**:
   - Upload the split files to the corresponding folders in GCS.

4. **Utility Functions**:
   - List blobs (files) in the GCS bucket for verification.

---

## **2. Prerequisites**
- Install required Python packages:
  ```bash
  pip install pandas google-cloud-storage
  ```

### **Set up Google Cloud SDK**
1. **Install and Authenticate the SDK**:
   - Follow the [official guide](https://cloud.google.com/sdk/docs/install) to install the Google Cloud SDK on your machine.
   - Authenticate with your Google Cloud account:
     ```bash
     gcloud auth login
     ```

2. **Set the Google Application Credentials**:
   - Download the JSON key file for your service account from the Google Cloud Console.
   - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the key file:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
     ```
   - Verify the authentication works:
     ```bash
     gcloud auth application-default login
     ```

3. **Set the Project ID**:
   - Set the active project for your Google Cloud SDK:
     ```bash
     gcloud config set project PROJECT_ID
     ```
   - Replace `PROJECT_ID` with your Google Cloud project ID.

### **Service Account Permissions**

The service account used for this script must have the following IAM roles for the target GCS bucket:
1. **Storage Admin** (for bucket creation and configuration).
2. **Storage Object Admin** (for creating folders and uploading files).

To grant permissions:
- Navigate to **IAM & Admin > IAM** in the Google Cloud Console.
- Click **Add** and assign the roles to the service account.

---
## **3. Step-by-Step Breakdown**

### **3.1. Bucket Management**

1. **Create a Bucket**:
    - The `create_bucket_class_location` method creates a GCS bucket in the US region with the `STANDARD` storage class.
    - Handles conflicts (e.g., bucket already exists) gracefully using the `google.api_core.exceptions.Conflict` exception.
2. **Simulate Folder Structure in GCS**:
    
    - GCS doesn’t support actual folders. Instead, "folders" are simulated by creating empty blobs (files) with a `/` suffix (e.g., `folder_name/`).

### **3.2. Data Processing**

1. **Split by Region**:
    - Filter the data based on unique values in the `Country` column.
    - Save each region’s data as a CSV file in a local folder (e.g., `Data/region_folder/`).
2. **Split by Date**:
    - Parse the `InvoiceDate` column into separate `Date` and `Time` columns.
    - Filter the data for each unique date and save as CSV files (e.g., `Data/date_folder/`).
3. **Local Directory Setup**:
    - Creates a `Data` folder and subfolders for each region or date if they don’t already exist.

### **3.3. Data Upload**

- The `upload_blob` method uploads files from the local `Data` folder to the corresponding "folders" in GCS.

### **3.4. List Files in GCS**

- Use the `list_blobs` method to list all objects in the bucket to verify uploads.

---
## **4. Key Methods**

| Method Name                    | Purpose                                                                                         |
| ------------------------------ | ----------------------------------------------------------------------------------------------- |
| `create_bucket_class_location` | Creates a new GCS bucket. Handles conflicts if the bucket already exists.                       |
| `create_folder_in_bucket`      | Simulates folders in GCS by uploading dummy blobs.                                              |
| `upload_blob`                  | Uploads a local file to a specific GCS folder.                                                  |
| `list_blobs`                   | Lists all files (blobs) in the specified GCS bucket.                                            |
| `split_by_region`              | Splits the CSV file based on the `Country` column and saves/upload files.                       |
| `split_by_day`                 | Splits the CSV file based on the `Date` parsed from the `InvoiceDate` column and saves/uploads. |

---

## **5. Script Usage**

### **Command Line Arguments**

| Argument            | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `--input_file_path` | Path to the input CSV file.                              |
| `--bucket_name`     | Name of the GCS bucket to create/use.                    |
| `--blob_list`       | Comma-separated list of "folders" (e.g., `region,date`). |

### **Example Command**

``` bash
python data_processor_gcs_uploader.py \
--input_file_path=Online_Retail.csv \
--bucket_name=online-retail-invoice \
--blob_list=online_retail_invoice_region_wise,online_retail_invoice_day_wise
```

## **6. Folder and File Structure**

### **Local Directory Structure**

``` yaml
Data/ 
├── region/ 
│   ├── USA-Invoice.csv 
│   ├── UK-Invoice.csv 
│   └── ... 
└── date/     
	 ├── 2024-11-15-Invoice.csv     
	 ├── 2024-11-16-Invoice.csv     
	 └── ...
```


### **GCS Folder Structure**


``` yaml
online-retail-invoice/ 
├── online_retail_invoice_region_wise/ 
│   ├── Australia-Invoice.csv 
│   ├── Austira-Invoice.csv 
│   └── ... 
└── online_retail_invoice_day_wise/     
	├── 2010-12-01-Invoice.csv     
	├── 2010-12-02-Invoice.csv     
	└── ...
```

## **Error Handling**

- **Bucket Creation**:
    - Handles conflicts if the bucket already exists.
- **Folder Creation**:
    - Skips folder creation if it already exists locally.
- **Data Upload**:
    - Uses `if_generation_match=0` to prevent overwriting existing files in GCS.

