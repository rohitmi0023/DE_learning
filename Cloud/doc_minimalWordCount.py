from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import SetupOptions
import re

argv=None
save_main_session=True
input_file = 'gs://dataflow-samples/shakespeare/kinglear.txt'
output_file = 'gs://my-bucket/counts.txt'

"""Main entry point; defines and runs the wordcount pipeline."""

parser = argparse.ArgumentParser()
parser.add_argument(
    '--input',
    dest='input',
    default='gs://dataflow-samples/shakespeare/kinglear.txt',
    help='Input file to process.')
parser.add_argument(
    '--output',
    dest='output',
    # CHANGE 1/6: (OPTIONAL) The Google Cloud Storage path is required
    # for outputting the results.
    default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
    help='Output file to write results to.')

# If you use DataflowRunner, below options can be passed:
#   CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
#   run your pipeline on the Google Cloud Dataflow Service.
#   '--runner=DirectRunner',
#   CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
#   run your pipeline on the Google Cloud Dataflow Service.
#   '--project=SET_YOUR_PROJECT_ID_HERE',
#   CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
#   is required in order to run your pipeline on the Google Cloud
#   Dataflow Service.
#   '--region=SET_REGION_HERE',
#   CHANGE 5/6: Your Google Cloud Storage path is required for staging local
#   files.
#   '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
#   CHANGE 6/6: Your Google Cloud Storage path is required for temporary
#   files.
#   '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
#   '--job_name=your-wordcount-job',
known_args, pipeline_args = parser.parse_known_args(argv)

# We use the save_main_session option because one or more DoFn's in this
# workflow rely on global context (e.g., a module imported at module level).
pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

pipeline = beam.Pipeline(options=pipeline_options)
with pipeline as pipeline:
  (
  pipeline 
  | 'Read' >> beam.io.ReadFromText(input_file) # Produces PCollection as output
  | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Splits line in PCollection<String>
  | 'Count' >> beam.combiners.Count.PerElement() # returns PCollection of key-value pairs
  | 'Format' >> beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
  | 'Write' >> beam.io.WriteToText(output_file)  
  )  

print('Success')