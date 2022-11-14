import boto3
import os

glue = boto3.client('glue')
job_name = os.environ['JOB_NAME']


def lambda_handler(event, context):
    glue.start_job_run(
        JobName=job_name,
        AllocatedCapacity=123,
        Timeout=120,
        WorkerType='G.1X',
        NumberOfWorkers=2,
        ExecutionClass='FLEX'
    )
