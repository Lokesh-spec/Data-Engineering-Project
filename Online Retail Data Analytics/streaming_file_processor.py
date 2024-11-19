import logging
import json
import csv
import datetime
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.window import FixedWindows
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        try:
            invoice_date = element["InvoiceDate"]
            logging.info(f"Processing timestamp for InvoiceDate: {invoice_date}")
            yield beam.window.TimestampedValue(element, invoice_date.timestamp())
        except KeyError as e:
            logging.error(f"Missing InvoiceDate in element: {element}, Error: {e}")
        except Exception as e:
            logging.error(f"Error adding timestamp: {e}")

def process_file(element):
    message = json.loads(element)
    bucket_name = message['bucket']
    file_name = message['name']

    gcs_path = f"gs://{bucket_name}/online_retail_invoice_region_wise/{file_name}"
    logging.info(f"Processing file from GCS: {gcs_path}")
    gcsio = GcsIO()
    try:
        file_content = gcsio.open(gcs_path, 'r').read().decode('utf-8')
        logging.info(f"File {file_name} read successfully.")
        yield from csv.DictReader(file_content.splitlines())
    except Exception as e:
        logging.error(f"Error reading file {file_name} from bucket {bucket_name}: {e}")

def parse_and_filter_csv_row(element):
    try:
        if "," in element["Description"]:
            element["Description"] = " ".join(element["Description"].split(","))

        if int(element["Quantity"]) < 0 or float(element["UnitPrice"]) <= 0.0 or not element.get("CustomerID"):
            logging.warning(f"Invalid row filtered out: {element}")
            return None

        parsed_row = {
            "InvoiceNo": str(element["InvoiceNo"]),
            "StockCode": str(element["StockCode"]),
            "Description": str(element["Description"]),
            "Quantity": int(element["Quantity"]),
            "InvoiceDate": datetime.strptime(element["InvoiceDate"], '%Y-%m-%d %H:%M:%S'),
            "UnitPrice": float(element["UnitPrice"]),
            "CustomerID": str(int(float(element["CustomerID"]))),
            "Country": str(element["Country"]),
        }
        logging.debug(f"Parsed row: {parsed_row}")
        return parsed_row
    except (ValueError, KeyError) as e:
        logging.error(f"Error parsing row {element}: {e}")
        return None

def compute_sales(element):
    try:
        element["Sales"] = round(element["Quantity"] * element["UnitPrice"], 2)
        logging.debug(f"Computed sales for element: {element}")
        return element
    except Exception as e:
        logging.error(f"Error computing sales for element {element}: {e}")

def prepare_for_bigquery_country_sales(element, window=beam.DoFn.WindowParam):
    window_start = window.start.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S')
    window_end = window.end.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Preparing country sales summary for window {window_start} - {window_end}")
    return {
        "start_date_time": window_start,
        "end_date_time": window_end,
        "country": element[0],
        "total_sales": round(element[1], 2)
    }

def prepare_for_bigquery_customer_sales(element, window=beam.DoFn.WindowParam):
    window_start = window.start.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S')
    window_end = window.end.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Preparing customer sales summary for window {window_start} - {window_end}")
    return {
        "start_date_time": window_start,
        "end_date_time": window_end,
        "customer": element[0],
        "total_sales": round(element[1], 2)
    }

def get_args():
    parser = argparse.ArgumentParser(description="Run Apache Beam pipeline on Dataflow")
    parser.add_argument('--runner', required=True, help="Pipeline runner (e.g., DataflowRunner, DirectRunner)")
    parser.add_argument('--project', required=True, help="GCP project ID")
    parser.add_argument('--region', required=True, help="GCP region for running the job")
    parser.add_argument('--temp_location', required=True, help="GCS path for temporary files")
    parser.add_argument('--staging_location', required=True, help="GCS path for staging files")
    parser.add_argument('--service_account', required=True, help="Service account email")
    parser.add_argument('--job_name', required=True, help="Unique name for the Dataflow job")
    parser.add_argument('--input_subscription', required=True, help="Pub/Sub subscription for input data")
    parser.add_argument('--country_table', required=True, help="BigQuery table for country sales summary")
    parser.add_argument('--customer_table', required=True, help="BigQuery table for customer sales summary")
    return parser.parse_args()

def run():
    args = get_args()
    options = PipelineOptions(
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location
    )
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.service_account_email = args.service_account
    google_cloud_options.job_name = args.job_name
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    logging.info("Starting Apache Beam pipeline.")

    with beam.Pipeline(options=options) as p:
        input_data = (
            p
            | 'Read from PubSub' >> ReadFromPubSub(subscription=args.input_subscription)
            | 'Process Files' >> beam.FlatMap(process_file)
            | 'Parse CSV Rows' >> beam.Map(parse_and_filter_csv_row)
            | 'Filter Valid Rows' >> beam.Filter(lambda x: x is not None)
            | 'Add Timestamps' >> beam.ParDo(AddTimestampDoFn())
            | 'Apply Fixed Window' >> beam.WindowInto(FixedWindows(3600))
            | 'Compute Sales' >> beam.Map(compute_sales)
        )

        # (
        #     input_data
        #     | 'Key By Country' >> beam.Map(lambda record: (record["Country"], record["Sales"]))
        #     | 'Aggregate Sales by Country' >> beam.CombinePerKey(sum)
        #     | 'Prepare for BigQuery Country Sales' >> beam.Map(prepare_for_bigquery_country_sales)
        #     | 'Write to BigQuery Country Table' >> WriteToBigQuery(
        #         table=args.country_table,
        #         schema={
        #             "fields": [
        #                 {"name": "start_date_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        #                 {"name": "end_date_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        #                 {"name": "country", "type": "STRING", "mode": "NULLABLE"},
        #                 {"name": "total_sales", "type": "FLOAT", "mode": "NULLABLE"}
        #             ]
        #         },
        #         write_disposition=BigQueryDisposition.WRITE_APPEND,
        #         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        #     )
        # )

        (
            input_data
            | 'Key By Customer' >> beam.Map(lambda record: (record["CustomerID"], record["Sales"]))
            | 'Aggregate Sales by CustomerID' >> beam.CombinePerKey(sum)
            | 'Prepare for BigQuery Customer Sales' >> beam.Map(prepare_for_bigquery_customer_sales)
            | 'Write to BigQuery Customer Table' >> WriteToBigQuery(
                table=args.customer_table,
                schema={
                    "fields": [
                        {"name": "start_date_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                        {"name": "end_date_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                        {"name": "customer", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "total_sales", "type": "FLOAT", "mode": "NULLABLE"}
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    logging.info("Apache Beam pipeline completed successfully.")

if __name__ == '__main__':
    run()
