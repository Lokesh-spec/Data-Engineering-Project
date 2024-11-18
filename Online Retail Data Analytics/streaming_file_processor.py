import json
import csv
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam import window
from datetime import datetime
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

class AddTimestampDoFn(beam.DoFn):
    """
    Adds an event timestamp to each element based on the InvoiceDate field.
    """
    def process(self, element):
        invoice_date = element["InvoiceDate"]
        yield beam.window.TimestampedValue(element, invoice_date.timestamp())

def process_file(element):
    message = json.loads(element)
    bucket_name = message['bucket']
    file_name = message['name']

    gcs_path = f"gs://{bucket_name}/online_retail_invoice_region_wise/{file_name}"
    gcsio = GcsIO()
    file_content = gcsio.open(gcs_path).read().decode('utf-8')

    yield from csv.DictReader(file_content.splitlines())

def parse_and_filter_csv_row(element):
    if "," in element["Description"]:
        element["Description"] = " ".join(element["Description"].split(","))
    try:
        if int(element["Quantity"]) < 0 or element["UnitPrice"] == "0.0" or not element["CustomerID"]:
            return None 
        return {
            "InvoiceNo": str(element["InvoiceNo"]),
            "StockCode": str(element["StockCode"]),
            "Description": str(element["Description"]),
            "Quantity": int(element["Quantity"]),
            "InvoiceDate": datetime.strptime(element["InvoiceDate"], '%Y-%m-%d %H:%M:%S'),
            "UnitPrice": float(element["UnitPrice"]),
            "CustomerID": str(int(float(element["CustomerID"]))),
            "Country": str(element["Country"])
        }
    except ValueError:
        return None

def compute_sales(element):
    element["Sales"] = round(element["Quantity"] * element["UnitPrice"], 2)
    return element

def prepare_for_bigquery(element, window=beam.DoFn.WindowParam):
    window_start = window.start.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S')
    window_end = window.end.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S')
    return {
        "window_start": window_start,
        "window_end": window_end,
        "country": element[0],
        "total_sales": element[1]
    }

def run():
    # Define pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    
    # Set your GCP project and staging/temp locations
    google_cloud_options.project = 'project_id'  # Replace with your GCP project ID
    google_cloud_options.region = 'us-central1'       # Replace with your region
    google_cloud_options.temp_location = 'gs://streaming-datapipeline-temp/tmp/'  # Replace with your bucket
    google_cloud_options.staging_location = 'gs://streaming-data-pipeline-staging/staging/'  # Replace with your bucket
    
    # Set runner (DirectRunner for local testing)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DirectRunner'  # or 'DataflowRunner' for cloud
    
    output_table = "project_id:sales_analysis.country_sales_summary"

    with beam.Pipeline(options=options) as p:
        input = (
            p
            | 'Mock PubSub Messages' >> beam.Create([ 
                json.dumps({'bucket': 'online-retail-invoice', 'name': '2010-12-01-Invoice.csv'}),
                json.dumps({'bucket': 'online-retail-invoice', 'name': '2010-12-02-Invoice.csv'}),
                json.dumps({'bucket': 'online-retail-invoice', 'name': '2010-12-03-Invoice.csv'})
            ])
            | 'Process Files' >> beam.FlatMap(process_file)
            | 'Parse the Data Type' >> beam.Map(parse_and_filter_csv_row)
            | 'Filter Valid Rows' >> beam.Filter(lambda x: x is not None)
            | "Add Timestamps" >> beam.ParDo(AddTimestampDoFn())
            | "Apply Fixed Window" >> beam.WindowInto(beam.window.FixedWindows(3600))
            | 'Compute Sales' >> beam.Map(compute_sales)
        )
        
        country_sales = (
            input
            | "Key By Country" >> beam.Map(lambda record: (record["Country"], record["Sales"]))
            | "Aggregate Sales by Country" >> beam.CombinePerKey(sum)
            | "Prepare for BigQuery" >> beam.Map(prepare_for_bigquery)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output_table,
                schema={
                    "fields": [
                        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "country", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "total_sales", "type": "FLOAT", "mode": "REQUIRED"}
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()

