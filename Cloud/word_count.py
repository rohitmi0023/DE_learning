# new concepts:

# %%
# Specifying explicit DoFns
import apache_beam as beam
class FormatAsTextFn(beam.DoFn):
    def process(self, element):
        word, count = element
        return f"({word}, {count})"
formatted = counts | beam.ParDo(FormatAsTextFn())

# %%
# creating composite transforms
import re

@beam.ptransform_fn
def CountWords(pcoll):
    return (
        pcoll
        | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | 'Count' >> beam.combiners.Count.PerElement()
    )

counts = lines | CountWords()

# %%
# using parameterizable PipelineOptions
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    '--input-file',
    default='gs://...',
    help='The file path for the input text to process.'
)
parser.add_argument(
    '--output-path', required=True, help='The path prefix for output files.'
)
args, beam_args = parser.parse_known_args()

beam_options = PipelineOptions(beam_args)
with beam.Pipeline(options=beam_options) as pipeline:
    lines = pipeline | beam.io.ReadFromText(args.input_file)


# %%
# word_count using PTransform

import apached_beam as beam
import time
from apache_beam.options.pipeline_options import PipelineOptions
import re

input_file = 'gs://apache_beam_113/kinglear.txt'
bq_table = 'radiant-ion-464314-b8.minimal_word_count.wordcounts'

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='radiant-ion-464314-b8',
    job_name='wordcountjobbigquery' + str(int(time.time())),
    temp_location='gs://apache_beam_113/temp/',
    staging_location='gs://apache_beam_113/staging',
    region='us-central1',
    service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
    dataflow_service_options=['enable_preflight_validation=False']
)

# custom composite transform 
class CountWords(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'ExtractWords' >> beam.FlatMap(lambda line: re.findall(r'[A-Za-z\']+', line))
            | 'Count' >> beam.combiners.Count.PerElement()
            | 'Format' >> beam.Map(lambda word: {'word': word[0], 'count': word[1]})
        )

with beam.Pipeline(options=beam_options) as pipeline:
    results = (
        pipeline
        | 'Read' >> beam.io.ReadFromText(input_file)
        | 'ProcessText' >> CountWords()
    )
    results | 'WriteToBQ' >> beam.io.WriteToBigQuery(
        bq_table,
        schema='word:STRING, count:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )


#%% 
# Word Count using functions

import apache_beam as beam
import re
import time
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# --- 1. Define PTransform Functions for Modularity ---

def read_text_from_gcs(pipeline, input_file_path):
    """
    Reads text from a specified Google Cloud Storage path.
    Args:
        pipeline: The Beam Pipeline object.
        input_file_path: The GCS path to the input text file.
    Returns:
        A PCollection of strings, where each string is a line from the input file.
    """
    return pipeline | 'ReadFromGCS' >> beam.io.ReadFromText(input_file_path)

def extract_and_count_words(lines_pcollection):
    """
    Takes a PCollection of text lines, extracts words, and counts their occurrences.
    Args:
        lines_pcollection: A PCollection of strings (lines of text).
    Returns:
        A PCollection of (word, count) tuples.
    """
    return (
        lines_pcollection
        | 'ExtractWords' >> beam.FlatMap(lambda line: re.findall(r'[A-Za-z\']+', line))
        | 'CountWords' >> beam.combiners.Count.PerElement()
    )

def format_for_bigquery(word_counts_pcollection):
    """
    Formats a PCollection of (word, count) tuples into dictionaries suitable for BigQuery.
    Args:
        word_counts_pcollection: A PCollection of (word, count) tuples.
    Returns:
        A PCollection of dictionaries, each with 'word' asnd 'count' keys.
    """
    return word_counts_pcollection | 'FormatForBQ' >> beam.Map(lambda x: {
        'word': x[0],
        'count': x[1]
    })

def write_to_bigquery(data_pcollection, bq_table_spec, bq_schema):
    """
    Writes a PCollection of dictionaries to a BigQuery table.
    Args:
        data_pcollection: A PCollection of dictionaries to write.
        bq_table_spec: The BigQuery table specification (e.g., 'project:dataset.table').
        bq_schema: The schema for the BigQuery table (e.g., 'word:STRING, count:INTEGER').
    """
    return data_pcollection | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        bq_table_spec,
        schema=bq_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

# --- 2. Main Pipeline Execution Logic ---

def run_word_count_pipeline(
    input_file: str,
    bq_table: str,
    project_id: str,
    temp_location: str,
    staging_location: str,
    region: str,
    service_account_email: str,
    runner: str = 'DataflowRunner'
):
    """
    Executes the modularized Apache Beam word count pipeline.

    Args:
        input_file: GCS path to the input text file.
        bq_table: BigQuery table specification (e.g., 'project:dataset.table').
        project_id: Google Cloud Project ID.
        temp_location: GCS path for temporary files.
        staging_location: GCS path for staging files.
        region: Google Cloud region for Dataflow.
        service_account_email: Service account email for Dataflow runner.
        runner: The Beam runner to use (e.g., 'DataflowRunner', 'DirectRunner').
    """
    # Define pipeline options
    beam_options = PipelineOptions(
        runner=runner,
        project=project_id,
        job_name=f'wordcountjobmodular-{int(time.time())}', # Unique job name
        temp_location=temp_location,
        staging_location=staging_location,
        region=region,
        service_account_email=service_account_email,
        dataflow_service_options=['enable_preflight_validation=False']
    )

    # Define BigQuery schema
    bq_schema = 'word:STRING, count:INTEGER'

    # Create and run the pipeline using the modular functions
    with beam.Pipeline(options=beam_options) as pipeline:
        # Read input
        lines = read_text_from_gcs(pipeline, input_file)

        # Extract and count words
        word_counts = extract_and_count_words(lines)

        # Format for BigQuery
        formatted_data = format_for_bigquery(word_counts)

        # Write to BigQuery
        write_to_bigquery(formatted_data, bq_table, bq_schema)

        print(f"Pipeline submitted: {beam_options.get_all_options()['job_name']}")

# --- 3. Example Usage (How to run your modular pipeline) ---
if __name__ == '__main__':
    # Configuration parameters
    input_file_path = 'gs://apache_beam_113/kinglear.txt'
    bigquery_table_spec = 'radiant-ion-464314-b8.minimal_word_count.wordcounts'
    gcp_project_id = 'radiant-ion-464314-b8'
    gcs_temp_location = 'gs://apache_beam_113/temp/'
    gcs_staging_location = 'gs://apache_beam_113/staging'
    dataflow_region = 'us-central1'
    dataflow_service_account = 'beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com'

    # Call the main function to run the pipeline
    run_word_count_pipeline(
        input_file=input_file_path,
        bq_table=bigquery_table_spec,
        project_id=gcp_project_id,
        temp_location=gcs_temp_location,
        staging_location=gcs_staging_location,
        region=dataflow_region,
        service_account_email=dataflow_service_account,
        runner='DataflowRunner' # Use 'DirectRunner' for local testing
    )
