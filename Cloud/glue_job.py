# Project description:
# 1. Trigger a Glue Job to clean and transform the data

import aws_glue
import sys
import pyspark
import boto3

args = aws_glue.utils.getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = pyspark.context.SparkContext()
glueContext = aws_glue.context.GlueContext(sc)
spark = glueContext.spark_session
job = aws_glue.job.Job(glueContext)
job.init(args['JOB_NAME'], args)

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    job_name = 'gluejob12'
    # start the Glue ETL Job
    response = glue_client.start_job_name(JobName=job_name)
    print(f"Started Glue Job: {response['JobRunId']}")


# Load data from S3
raw_data = glueContext.create_dynamic_frame.from_options(
    's3',
    {'paths': ['s3://raw-data/'], 'recurse': True},
    format='csv'
)

# transformation
cleaned_data = aws_glue.transforms.Filter.apply(frame=raw_data
    , f= lambda x: x['column_name'] is not None
)

# loading to another S3
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_data,
    connection_type='s3',
    connection_options={'path': 's3://cleaned_data/'},
    form='parquet'
)

job.commit()

# 2. Automate Triggering Glue Job with Lambda
# Lambda function will trigger the Glue job when the raw data is updated
import boto3

# def lambda_handler(event, context):
#     glue_client = boto3.client('glue')
#     response = glue_client.start_job_run(jobName='gluejob123')
#     return response

def lambda_handler(event, context):
    s3 = boto.client('s3')
    # extract bucket and object key from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')

    sns = boto3.client('sns')
    sns.publish(
        TopicArn='arn:aws:sns:region:account_id:YourTopic',
        Message='Data validation failed. Please check the Glue output.',
        Subject='Data Processing Failed'
    )

# 3. Query Processed with Athena
# create a table in Athena pointing to the output location
select *
from processed_data
where column_name = 'value'
;

# [ Raw Data in S3 ] --(Event)--> [ Lambda ] --(Start Job)--> [ Glue ETL Job ] --(Write)--> [ Processed Data S3 ]
#                        |                                         |
#                        +--> [ Athena Query ]                     +--> [ BI/Visualization Tools ]






# %%

dic = {'name': 'rohit'}

def unpack(**dict):
    print(**dict)
    unpacked = set(dict)
    print(unpacked)

