# description- process and extract information from log file. Each line contains server request details
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import time

input_file_path = 'gs://web_server_logs2025/log1.txt'

# beam_options = PipelineOptions(
#   runner='DataflowRunner',
#   project='radiant-ion-464314-b8',
#   job_name=f'webserveranalysis{str(int(time.time()))}',
#   temp_location='gs://web_server_logs2025/temp',
#   staging_location='gs://web_server_logs2025/staging',
#   region='us-central1',
#   service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
#   dataflow_service_options=['enable_preflight_validation=False']
# )

beam_options = PipelineOptions(
  runner='DirectRunner',
  project='radiant-ion-464314-b8'
#   job_name=f'webserveranalysis{str(int(time.time()))}',
#   temp_location='gs://web_server_logs2025/temp',
#   staging_location='gs://web_server_logs2025/staging',
#   region='us-central1',
#   service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
#   dataflow_service_options=['enable_preflight_validation=False']
)

def ExtractLogs(lines_pcoll):
  # arrays = map(lambda line: line.split(), lines_pcoll)
  # ip_address = arrays[0]
  # print(ip_address)
  return (
    lines_pcoll
    | 'Parsing' >> beam.Map(lambda line: line.split())[0]
	)
  

with beam.Pipeline(options=beam_options) as pipeline:
  read = pipeline | 'ReadLogs' >> beam.io.ReadFromText(input_file_path) # 1. Reading From GCP Bucket
  print('Read from GCP Bucket Successfully')
  transforms = ExtractLogs(read)
  print('Ran Parsing PTransform Successfully')