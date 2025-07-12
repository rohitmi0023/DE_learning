# Direct Runner
# description- process and extract information from log file. Each line contains server request details
# Key Concepts to Implement:
# 1. eading from GCP Bucket
# 2. Custom Transform (Parsing): Extract Relavant fields with DoFn/Map, handle potential erros gracefully, create python dict or dataclass
# 3. Filter to show 200 status codes
# 4. Simple Aggregation: Most Popular Pages, Unique IP Addresses
# 5. Wrtie to BigQuery


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import time
import re
from datetime import datetime

input_file_path = 'gs://web_server_logs2025/log1.txt'

beam_options = PipelineOptions(
  runner='DirectRunner',
  project='radiant-ion-464314-b8',
  # job_name='webserverlogsjobbigquery' + str(int(time.time())),
  temp_location='gs://apache_beam_113/temp/',
  staging_location='gs://apache_beam_113/staging',
  # region='us-central1',
  service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
  dataflow_service_options=['enable_preflight_validation=False']    
)

def ExtractLogs(lines_pcoll):
  # return (
  #   lines_pcoll
  #   | 'Parsing' >> beam.Map(lambda line: line.split(' ')[0]) --returning just IP Address
  # )
  return(
    lines_pcoll
    | 'Parsing' >> beam.Map(lambda line: regex_extract(line))
    | 'Filtering' >> beam.Filter(lambda line: line is not None)
  ) 

def regex_extract(log_line):
  format_string = '%d/%b/%Y:%H:%M:%S %z'
  match = re.match(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(.*?)] "(.*?)" (\d+) (\d+) "-" "(.*?)" "-"', log_line)
  if match:
    return {
      'ip_address': match.group(1),
      'timestamp': datetime.strptime(match.group(2), format_string),
      'request_type': match.group(3).split(' ')[0],
      'request_path': match.group(3).split(' ')[1],
      'status_code': int(match.group(4)),
      'response_size': int(match.group(5)),
      'referrer': match.group(6)
    }
  else:
#     print(f'Could not parse line: {log_line}')
    return None

def filtered200(p_coll):
  return(
    p_coll
    | beam.Filter(lambda keys: (keys['status_code']==200))
  )

def page_count(pcoll):
  return(
    pcoll
    | 'request_path' >> beam.Map(lambda line: line['request_path'])
    | 'Count' >> beam.combiners.Count.PerElement()
  )

bigquerytableschema = {
  'fields': [
    {'name': 'ip_address', 'type': 'STRING', 'mode':'REQUIRED'},
    {'name': 'timestamp', 'type': 'DATETIME', 'mode':'REQUIRED'},
    {'name': 'request_type', 'type': 'STRING', 'mode':'REQUIRED'},
    {'name': 'request_path', 'type': 'STRING', 'mode':'REQUIRED'},
    {'name': 'status_code', 'type': 'INTEGER', 'mode':'REQUIRED'},
    {'name': 'response_size', 'type': 'INTEGER', 'mode':'REQUIRED'},
    {'name': 'referrer', 'type': 'STRING', 'mode':'REQUIRED'},
  ]
}

with beam.Pipeline(options=beam_options) as pipeline:
  read = pipeline | 'ReadLogs' >> beam.io.ReadFromText(input_file_path) # 1. Reading From GCP Bucket
  transforms = ExtractLogs(read)
  transforms | 'Print Transforms' >> beam.Map(print)
  filter200 = filtered200(transforms)
  # filter200 | 'Print200' >> beam.Map(print)
  visited_pages = page_count(filter200)  
  visited_pages | 'Count Per page' >> beam.Map(print)
  transforms | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    table='radiant-ion-464314-b8.minimal_word_count.pages_count_table',
    # schema=bigquerytableschema,
    schema='ip_address: STRING, timestamp: DATETIME, request_type: STRING, request_path: STRING, status_code: INTEGER, response_size: INTEGER, referrer: STRING'
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
  )  
  