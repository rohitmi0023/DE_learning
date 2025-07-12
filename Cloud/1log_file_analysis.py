import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition
import re
import logging # Good practice for Beam pipelines

# Set up logging for your pipeline
logging.basicConfig(level=logging.INFO)

input_file_path = 'gs://web_server_logs2025/log1.txt'
# Replace with your actual project, dataset, and table IDs
output_table_id = 'radiant-ion-464314-b8:log_analysis.web_access_logs' 

beam_options = PipelineOptions(
  runner='DirectRunner', # Use 'DataflowRunner' for production
  project='radiant-ion-464314-b8',
  # Add other options like staging_location, temp_location for DataflowRunner
  # staging_location='gs://your_bucket/staging',
  # temp_location='gs://your_bucket/temp'
)

# Define the BigQuery schema for your output table
# This matches the dictionary keys and types from parse_log_line
bigquery_schema = {
  'fields': [
    {'name': 'ip_address', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'timestamp_str', 'type': 'STRING', 'mode': 'NULLABLE'}, # Storing as string for simplicity
    {'name': 'request_path', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'status_code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'response_size', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'referrer', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'user_agent', 'type': 'STRING', 'mode': 'NULLABLE'}
  ]
}

def parse_log_line(log_line):
  # A simple regex for the example log line.
  # For real-world logs, this might be more complex or use a dedicated log parser library.
  # Regex groups: (IP) - - [(timestamp)] "(request_method path protocol)" (status) (size) "(referrer)" "(user_agent)"
  match = re.match(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"', log_line)
  if match:
    try:
        # Extract request path from "GET /path HTTP/1.1"
        full_request = match.group(3)
        request_parts = full_request.split(' ')
        request_path = request_parts[1] if len(request_parts) > 1 else full_request

        return {
            'ip_address': match.group(1),
            'timestamp_str': match.group(2),
            'request_path': request_path,
            'status_code': int(match.group(4)),
            'response_size': int(match.group(5)),
            'referrer': match.group(6),
            'user_agent': match.group(7)
        }
    except (ValueError, IndexError) as e:
        logging.warning(f"Error parsing values in line: {log_line} - {e}")
        return None
  else:
    logging.warning(f"Could not match regex for line: {log_line}")
    return None

def ExtractAndTransformLogs(lines_pcoll):
  return (
      lines_pcoll
      | 'Parse Log Line' >> beam.Map(parse_log_line)
      | 'Filter None (Malformed Lines)' >> beam.Filter(lambda element: element is not None)
  )
    
with beam.Pipeline(options=beam_options) as pipeline:
  read_logs = pipeline | 'ReadLogs' >> beam.io.ReadFromText(input_file_path)
  logging.info('Read from GCP Bucket Successfully')
  
  # PCollection of dictionaries, each representing a parsed log entry
  parsed_log_entries = ExtractAndTransformLogs(read_logs)
  logging.info('Ran Parsing PTransform Successfully')

  # Write the processed data to BigQuery
  parsed_log_entries | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
      table=output_table_id,
      schema=bigquery_schema,
      write_disposition=BigQueryDisposition.WRITE_TRUNCATE, # Overwrite table if it exists
      create_disposition=BigQueryDisposition.CREATE_IF_NEEDED # Create table if it doesn't exist
  )
  logging.info(f'Wrote data to BigQuery table: {output_table_id}')