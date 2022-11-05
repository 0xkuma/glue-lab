import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME", "connection_options"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

ssm = boto3.client('ssm')
response = ssm.get_parameter(Name='/KL/labels', WithDecryption=False)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["{}/input".format(args['connection_options'])], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Filter
Filter_node1667356797206 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (bool(re.match("CW:Requests", row["UsageType"]))),
    transformation_ctx="Filter_node1667356797206",
)

# Script generated for node Amazon S3
AmazonS3_node1667289496050 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1667356797206.coalesce(1),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "{}/output/requests/".format(args['connection_options']),
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1667289496050",
)

job.commit()
