import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import pandas as pd
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO

# Replace these values with your specific GCS and BigQuery information.
GCS_BUCKET = 'your-gcs-bucket'
BQ_PROJECT = 'your-project-id'
BQ_DATASET = 'your-dataset-id'
BQ_TABLE = 'your-table-id'

class ReadCSVFile(beam.DoFn):
    def process(self, element):
        # Read the CSV file from GCS and convert to a Pandas DataFrame
        gcs_path = f'gs://{GCS_BUCKET}/{element}'
        gcs = GcsIO()
        with gcs.open(gcs_path) as file:
            df = pd.read_csv(file)

        # Implement your data transformation logic here
        # Example: Add a new column to the DataFrame
        df['new_column'] = df['existing_column'] * 2

        # Iterate over DataFrame rows and emit each row
        for _, row in df.iterrows():
            yield row

def run():
    # Define the pipeline options
    options = PipelineOptions()

    # Set up Google Cloud options (project and region)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = BQ_PROJECT
    google_cloud_options.region = 'us-central1'  # Change to your desired region

    # Set up standard options (runner, streaming, etc.)
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'

    # Create the Pipeline
    with beam.Pipeline(options=options) as p:
        # List the CSV files in the GCS bucket
        csv_files = (p
            | 'List CSV Files' >> beam.Create(["your-csv-folder/file1.csv", "your-csv-folder/file2.csv"])  # Replace with your file paths
        )

        # Read and transform each CSV file
        transformed_data = (csv_files
            | 'Read and Transform CSV Files' >> beam.ParDo(ReadCSVFile())
        )

        # Write the transformed data to BigQuery
        transformed_data | 'Write to BigQuery' >> WriteToBigQuery(
            table=f'{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE}',
            schema='your-schema.json',  # Define your BigQuery schema
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run()
