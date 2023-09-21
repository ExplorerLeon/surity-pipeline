"""
Dataflow and Direct runner working etl workflow to exctract excel file
from GS bucket, transform and cleaning steps and write to Big Query Tabel.

Built in packaged folder structure to deploy on VM workers executing the pipeline
job. Setup.py file contains code to create package.

The command (in Dataflow) `--setup_file` is hard coded to create the package (as if running python
setup.py sdist) and then staging the resulting tarball in the staging area. The workers, upon
startup, will install the tarball.

Below is a complete command line for the workflow remotely as
an example:

python excel-etl-pipeline.py \
  --job_name <insert_name> \
  --project <YOUR-PROJECT> \
  --region <GCE-REGION> \
  --runner DataflowRunner \
  --setup_file ./setup.py \
  --temp_location gs://YOUR-BUCKET/<path>/<temp> \

"""

import pandas as pd
import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import DoFn

from utils.transform import transformation

# import dotenv
# dotenv.load_dotenv()

PROJECT_ID = 'assetinsure-surety-data-models'
BUCKET = 'gs://surety-data-models'
TABLE_SPEC = f'{PROJECT_ID}:ls_panthers_test.panters-test-table-1'

# table_schema = {
#         'fields': [
#             {'name': 'ID', 'type': 'NUMERIC'},
#             {'name': 'CompanyName', 'type': 'STRING'},
#             {'name': 'Date', 'type': 'DATETIME'}
#         ]
#     }

table_schema = {
        'fields': [
            {'name': 'number', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'date', 'type': 'TIMESTAMP'},
            {'name': 'value', 'type': 'FLOAT'},
            {'name': 'boolean', 'type': 'BOOLEAN'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}
            ]
    }

class ReadExcelFile(DoFn):
    def process(self, element):

        # Read the Excel file from GCS and convert to a Pandas DataFrame
        gcs_path = f'{BUCKET}/{element}'
        gcs = GcsIO()
        with gcs.open(gcs_path) as file:
            df = pd.read_excel(file)

        # Apply transformations and cleaning steps
        df = transformation(df)

        # Yield rows
        # Iterate over DataFrame rows and emit each row
        for _, row in df.iterrows():
            yield row.to_dict()

def run(argv=None, save_main_session = True):
    parser = argparse.ArgumentParser()
    # parser.add_argument('--my-arg', help='description')
    args, beam_args = parser.parse_known_args()

    # Create and set your PipelineOptions.
    # For Cloud execution, specify DataflowRunner and set the Cloud Platform
    # project, job name, temporary files location, and region.

    pipeline_options = PipelineOptions(
        beam_args,
        runner='Direct', # DataflowRunner or Direct
        project=PROJECT_ID,
        job_name='test-6-book-ls',
        temp_location=f'{BUCKET}/temp/',
        region='europe-west2',
        setup_file = "./setup.py")

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # List the Excel files in the GCS bucket
        excel_files = (p
            | 'List Excel Files' >> beam.Create(["input/Book.xlsm"])  # Replace with your file paths
        )

        # Read and transform each Excel file
        transformed_data = (excel_files
            | 'Read and Transform Excel Files' >> beam.ParDo(ReadExcelFile())
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
