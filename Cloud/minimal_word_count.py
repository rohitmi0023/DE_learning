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
# Must give BigQuery Data Editor access to service account
import apache_beam as beam
import re 
from apache_beam.options.pipeline_options import PipelineOptions
import time as t

input_file = 'gs://apache_beam_113/kinglear.txt'
output_prefix = 'gs://apache_beam_113/beam_playground_test'

beam_options = PipelineOptions(
   runner='DataflowRunner',
   project='radiant-ion-464314-b8',
   job_name='wordcountjob' + str(int(t.time())),
   temp_location='gs://apache_beam_113/temp/',
   staging_location='gs://apache_beam_113/staging',
   region='us-central1',
   service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
   dataflow_service_options=['enable_preflight_validation=False']
)

with beam.Pipeline(options=beam_options) as pipeline:
  (
    pipeline
    | 'Read' >> beam.io.ReadFromText(input_file)
    | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
    | 'Count' >> beam.combiners.Count.PerElement()
    | 'Format' >> beam.MapTuple(lambda word, count: f'{word}: {count}')
    | 'Write' >> beam.io.WriteToText(
        output_prefix,
        file_name_suffix='.txt',
        num_shards=1
    )
  )
# %%
# MinimalWordCount -> Writes to BigQuery -> Runs using GCP runner

import apache_beam as beam
import re 
from apache_beam.options.pipeline_options import PipelineOptions
import time

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

with beam.Pipeline(options=beam_options) as pipeline:
  (
    pipeline
    | 'Read' >> beam.io.ReadFromText(input_file)
    | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
    | 'Count' >> beam.combiners.Count.PerElement()
    | 'Format' >> beam.Map(lambda x: {
       'word': x[0],
       'count': x[1]
    })
    | 'WriteToBQ' >> beam.io.WriteToBigQuery(
       bq_table,
       schema='word:STRING, count:INTEGER',
       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )
    
  )
