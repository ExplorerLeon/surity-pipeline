python3 -m \
    apache_beam.examples.wordcount \
    --region europe-west2 --input \
    gs://dataflow-samples/shakespeare/kinglear.txt \
    --output \
    gs://surety-data-models/output \
    --runner DataflowRunner \
    --project \
    assetinsure-surety-data-models \
    --temp_location \
    gs://surety-data-models/temp/
