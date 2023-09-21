
import pandas as pd
import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.transforms import DoFn

# import dotenv
# dotenv.load_dotenv()

PROJECT_ID = 'assetinsure-surety-data-models'
BUCKET = 'gs://surety-data-models'
TABLE_SPEC = f'{PROJECT_ID}:ls_panthers_test.panters-test-table-1'

table_schema = {
        'fields': [
            {'name': 'ID', 'type': 'NUMERIC'},
            {'name': 'CompanyName', 'type': 'STRING'},
            {'name': 'Date', 'type': 'DATETIME'}
        ]
    }

class ReadCSVFile(DoFn):
    def process(self, element):
        # Read the CSV file from GCS and convert to a Pandas DataFrame
        gcs_path = f'{BUCKET}/{element}'
        gcs = GcsIO()
        with gcs.open(gcs_path) as file:
            df = pd.read_excel(file)

        # tranforms
        df = transform(df)

        # create schema


        # yield rows

        # Implement your data transformation logic here
        # Example: Add a new column to the DataFrame
        first_non_null_index = df[df.iloc[:, 0].notnull()].index[0]
        # Extract rows from the first non-null cell in the first column onwards
        company_name = df.iloc[first_non_null_index, 0]
        print(company_name)

        # Get the current date and time
        current_time = datetime.datetime.now()
        # Print the current time
        print(current_time)

        # Create a dictionary to represent the row data
        row_data = {
            'ID': [2,3,4],  # Replace with the actual ID if available
            'CompanyName': [company_name,company_name,company_name],
            'Date': [current_time,current_time,current_time]
            }

        df = pd.DataFrame.from_dict(row_data, orient='index').T
        # Iterate over DataFrame rows and emit each row
        for _, row in df.iterrows():
            yield row.to_dict()

        # Iterate over DataFrame rows and emit each row
        # yield row_data

def run(argv=None, save_main_session = True):
    parser = argparse.ArgumentParser()
    # parser.add_argument('--my-arg', help='description')
    args, beam_args = parser.parse_known_args()

    # Create and set your PipelineOptions.
    # For Cloud execution, specify DataflowRunner and set the Cloud Platform
    # project, job name, temporary files location, and region.
    # For more information about regions, check:
    # https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    pipeline_options = PipelineOptions(
        beam_args,
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name='test-4-dftodic-ls',
        temp_location=f'{BUCKET}/temp/',
        region='europe-west2')

    # Note: Repeatable options like dataflow_service_options or experiments must
    # be specified as a list of string(s).
    # e.g. dataflow_service_options=['enable_prime']
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # List the CSV files in the GCS bucket
        csv_files = (p
            | 'List CSV Files' >> beam.Create(["input/Panthers Financial Model Oct-22.xlsm"])  # Replace with your file paths
        )

        # Read and transform each CSV file
        transformed_data = (csv_files
            | 'Read and Transform CSV Files' >> beam.ParDo(ReadCSVFile())
        )

        # Write the transformed data to BigQuery
        transformed_data | 'Write to BigQuery' >> WriteToBigQuery(
            table=TABLE_SPEC,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location=f'{BUCKET}/temp/'
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
