import json
import csv
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
from collections import namedtuple
import logging

# Define the timestamp for dynamic file naming
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# Define output path
output_path = f"./processed/results-{timestamp}"

# Define a NamedTuple to represent the schema of the CSV rows
Row = namedtuple(
    'Row', ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
)

# Replace None or empty values with default values
def handle_missing_values(row, default_values=None):
    default_values = default_values or {}
    return {
        key: (value if value else default_values.get(key, 'Unknown')) for key, value in row.items()
    }

# Updated filter_out_nulls function
def filter_out_nulls(row, critical_fields):
    return all(row.get(field) and row[field].strip() for field in critical_fields)

# Updated ProcessData class with logging
class ProcessData(beam.DoFn):
    def __init__(self, default_values, critical_fields):
        self.default_values = default_values
        self.critical_fields = critical_fields

    def process(self, element):
        logging.info(f"Processing row: {element}")
        element = handle_missing_values(element, self.default_values)

        if filter_out_nulls(element, self.critical_fields):
            logging.info(f"Valid row: {element}")
            yield element
        else:
            logging.warning(f"Invalid row: {element}")
            yield {**element, 'status': 'Invalid'}

# Read and process CSV files from GCS
def process_file(element):
    # sourcery skip: inline-immediately-yielded-variable, yield-from
    message = json.loads(element)
    bucket_name = message['bucket']
    file_name = message['name']

    gcs_path = f"gs://{bucket_name}/online_retail_invoice_region_wise/{file_name}"
    gcsio = GcsIO()
    file_content = gcsio.open(gcs_path).read().decode('utf-8')

    # Use csv.DictReader for structured row parsing
    reader = csv.DictReader(file_content.splitlines())
    for row in reader:
        yield row

# Format rows into CSV strings for output
def format_row(row):
    return f"{row['InvoiceNo']},{row['StockCode']},{row['Description']},{row['Quantity']},{row['InvoiceDate']},{row['UnitPrice']},{row['CustomerID']},{row['Country']}"

def run():
    # Define pipeline options
    options = PipelineOptions(
        runner="DirectRunner",  # Use DataflowRunner for production
        temp_location='./temp/',  # Temporary location for intermediate data
        save_main_session=True  # Save main session for pickling
    )

    default_values = {
        'InvoiceNo': 'Unknown',
        'StockCode': 'Unknown',
        'Description': '',
        'Quantity': '0',
        'InvoiceDate': '1970-01-01',
        'UnitPrice': '0.0',
        'CustomerID': 'Unknown',
        'Country': 'Unknown'
    }
    critical_fields = ['InvoiceNo', 'StockCode', 'Quantity', 'InvoiceDate']

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Mock PubSub Messages' >> beam.Create([
                json.dumps({'bucket': 'online-retail-invoice', 'name': '2010-12-01-Invoice.csv'}),
                json.dumps({'bucket': 'online-retail-invoice', 'name': '2010-12-02-Invoice.csv'}),
                json.dumps({'bucket': 'online-retail-invoice', 'name': '2010-12-03-Invoice.csv'})
            ])

            | 'Process Files' >> beam.FlatMap(process_file)
            | 'Clean and Validate Data' >> beam.ParDo(ProcessData(default_values, critical_fields))
            | 'Format to CSV' >> beam.Map(format_row)
            | 'Write Results' >> beam.io.WriteToText(output_path, file_name_suffix='.csv')
        )

if __name__ == '__main__':
    run()
