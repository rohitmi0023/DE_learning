import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import time
import re
from datetime import datetime

# Input file path from GCP Bucket
input_file_path = 'gs://web_server_logs2025/log1.txt'

# Apache Beam Pipeline Options for DataflowRunner
beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='radiant-ion-464314-b8',
    job_name='webserverlogsjobbigquery' + str(int(time.time())), # Unique job name
    temp_location='gs://apache_beam_113/temp/',
    staging_location='gs://apache_beam_113/staging',
    region='us-central1',
    service_account_email='beam-runner@radiant-ion-464314-b8.iam.gserviceaccount.com',
    dataflow_service_options=['enable_preflight_validation=False']
)

class ExtractLogs(beam.PTransform):
  """
  A custom PTransform to parse log lines using regex and filter out unparseable lines.
  """
  def expand(self, lines_pcoll):
    return (
        lines_pcoll
        | 'Parsing' >> beam.Map(regex_extract)
        | 'Filtering' >> beam.Filter(lambda line: line is not None)
    )

def regex_extract(log_line):
  """
  Extracts relevant fields from a log line using regex.
  Converts the timestamp to a string format suitable for BigQuery DATETIME type.
  Handles potential parsing errors by returning None.
  """
  # Regex pattern to match common Apache log format
  # Groups: IP, Timestamp, Request Line, Status Code, Response Size, Referrer, User Agent (not captured)
  # The regex was slightly adjusted to correctly capture the referrer.
  # Original: (\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(.*?)] "(.*?)" (\d+) (\d+) "-" "(.*?)" "-"
  # Adjusted to ensure referrer is correctly captured, especially if it's empty or a simple URL.
  # The last "-" was causing issues if the user agent was missing or different.
  # A more robust regex:
  # r'(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "([^"]*)" "([^"]*)"'
  # For the provided log format, the original regex is mostly fine, but let's make sure the last part is flexible.
  # The original regex: r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(.*?)] "(.*?)" (\d+) (\d+) "-" "(.*?)" "-"'
  # This regex assumes the last part is always "-". If it's not, it will fail.
  # Let's use a more flexible one for the last two fields (referrer and user agent)
  match = re.match(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(.*?)] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"', log_line)

  if match:
      try:
          # Define the timestamp format string
          format_string = '%d/%b/%Y:%H:%M:%S %z'
          # Parse the timestamp string into a datetime object
          dt_object = datetime.strptime(match.group(2), format_string)
          # Format the datetime object into a string suitable for BigQuery DATETIME
          # BigQuery DATETIME format: 'YYYY-MM-DD HH:MM:SS'
          timestamp_str = dt_object.strftime('%Y-%m-%d %H:%M:%S')

          request_parts = match.group(3).split(' ')
          request_type = request_parts[0] if len(request_parts) > 0 else ''
          request_path = request_parts[1] if len(request_parts) > 1 else ''

          return {
              'ip_address': match.group(1),
              'timestamp': timestamp_str, # Modified: Convert datetime object to string
              'request_type': request_type,
              'request_path': request_path,
              'status_code': int(match.group(4)),
              'response_size': int(match.group(5)),
              'referrer': match.group(6) # Referrer is group 6 in the new regex
          }
      except ValueError as e:
          print(f'Error parsing timestamp or other field in line: {log_line} - {e}')
          return None
  else:
      print(f'Could not parse line with regex: {log_line}')
      return None

def filtered200(p_coll):
  """
  Filters logs to include only those with a 200 status code.
  """
  return (
      p_coll
      | 'Filter200StatusCode' >> beam.Filter(lambda log_entry: log_entry['status_code'] == 200)
  )

def page_count(pcoll):
  """
  Counts the occurrences of each request path (page).
  """
  return (
      pcoll
      | 'ExtractRequestPath' >> beam.Map(lambda log_entry: log_entry['request_path'])
      | 'CountPageVisits' >> beam.combiners.Count.PerElement()
  )

def unique_ip_addresses(pcoll):
  """
  Finds unique IP addresses.
  """
  return (
      pcoll
      | 'ExtractIPAddress' >> beam.Map(lambda log_entry: log_entry['ip_address'])
      | 'GetUniqueIPs' >> beam.Distinct()
  )

# BigQuery table schema definition
bigquerytableschema = {
    'fields': [
        {'name': 'ip_address', 'type': 'STRING', 'mode':'REQUIRED'},
        {'name': 'timestamp', 'type': 'DATETIME', 'mode':'REQUIRED'}, # BigQuery DATETIME type
        {'name': 'request_type', 'type': 'STRING', 'mode':'REQUIRED'},
        {'name': 'request_path', 'type': 'STRING', 'mode':'REQUIRED'},
        {'name': 'status_code', 'type': 'INTEGER', 'mode':'REQUIRED'},
        {'name': 'response_size', 'type': 'INTEGER', 'mode':'REQUIRED'},
        {'name': 'referrer', 'type': 'STRING', 'mode':'REQUIRED'},
    ]
}

# Apache Beam Pipeline definition
with beam.Pipeline(options=beam_options) as pipeline:
  # 1. Reading from GCP Bucket
  read = pipeline | 'ReadLogs' >> beam.io.ReadFromText(input_file_path)

  # 2. Custom Transform (Parsing): Extract Relevant fields with DoFn/Map, handle potential errors gracefully
  transforms = read | 'ExtractAndFilterLogs' >> ExtractLogs()

  # Example of using the filtered200 and page_count transforms (uncommented for demonstration)
  filter200 = transforms | 'FilterFor200StatusCode' >> filtered200()
  # filter200 | 'Print200FilteredLogs' >> beam.Map(print) # Uncomment to print filtered 200 status logs

  visited_pages = filter200 | 'CountMostPopularPages' >> page_count()
  # visited_pages | 'PrintPageCounts' >> beam.Map(print) # Uncomment to print page counts

  unique_ips = transforms | 'CalculateUniqueIPs' >> unique_ip_addresses()
  # unique_ips | 'PrintUniqueIPs' >> beam.Map(print) # Uncomment to print unique IPs

  # 5. Write to BigQuery
  # This writes the parsed log entries (after initial filtering of unparseable lines) to BigQuery.
  # The 'timestamp' field is now a string in the correct format.
  transforms | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
      table='radiant-ion-464314-b8.webserverlogs.pages_count_table', # Ensure this table exists or will be created
      schema=bigquerytableschema,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE # Overwrites table if it exists
  )