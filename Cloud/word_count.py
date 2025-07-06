# %%
# basic structure

import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Read" >> beam.io.ReadFromText('input.txt')
        | "Split" >> beam.FlatMap(lambda line: line.split())
        | "Pair" >> beam.map(lambda x: (x,1))
        | 'Count' >> beam.CombinePerKey(sum)
        | 'Format' >> beam.map(lambda item: f"{item[0]}: {item[1]}")
        | 'Write' >> beam.io.WriteToText('output.txt')
    )
# Beam distributes the Count part among different workers per key

# %% 
# MinimalWordCount -> Ran using playground runner
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re

# Use gs:// paths instead of https:// for GCS access
input_file = 'gs://apache_beam_113/kinglear.txt'
output_prefix = 'gs://apache_beam_113/beam_playground' # Output prefix instead of full path

beam_options = PipelineOptions(
    runner = 'DirectRunner',
    project = 'radiant-ion-464314-b8',
    job_name = 'unique-job-name77794',
    temp_location ='gs://apache_beam_113/temp/',
    region = 'us-central1',
    staging_location = 'gs://apache_beam_113/staging'      
)


with beam.Pipeline(options=beam_options) as pipeline:
  (
    pipeline 
    | 'Read' >> beam.io.ReadFromText(input_file) # Produces PCollection as output
    | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Splits line in PCollection<String>
    | 'Count' >> beam.combiners.Count.PerElement() # returns PCollection of key-value pairs
    | 'Format' >> beam.MapTuple(lambda word, count: f'{word}: {count}')
    | 'Write' >> beam.io.WriteToText(
      output_prefix,
      file_name_suffix='.txt',
      num_shards=1 # Controls number of output files
    )
  )


# %% 
# MinimalWordCount -> Runs using GCP runner
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re

# Use gs:// paths instead of https:// for GCS access
input_file = 'gs://apache_beam_113/kinglear.txt'
output_prefix = 'gs://apache_beam_113/beam_playground' # Output prefix instead of full path

beam_options = PipelineOptions(
    runner = 'DataflowRunner',
    project = 'radiant-ion-464314-b8',
    job_name = 'unique-job-name777949',
    temp_location ='gs://apache_beam_113/temp/',
    region = 'us-central1',
    staging_location = 'gs://apache_beam_113/staging',
    service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
    dataflow_service_options=['enable_preflight_validation=false']
)


with beam.Pipeline(options=beam_options) as pipeline:
  (
    pipeline 
    | 'Read' >> beam.io.ReadFromText(input_file) # Produces PCollection as output
    | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Splits line in PCollection<String>
    | 'Count' >> beam.combiners.Count.PerElement() # returns PCollection of key-value pairs
    | 'Format' >> beam.MapTuple(lambda word, count: f'{word}: {count}')
    | 'Write' >> beam.io.WriteToText(
      output_prefix,
      file_name_suffix='.txt',
      num_shards=1 # Controls number of output files
    )
  )

