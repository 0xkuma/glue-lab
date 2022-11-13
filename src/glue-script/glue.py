import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
import boto3
import json

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "connection_options", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.resource('s3')
obj = s3.Bucket(args['bucket_name']).Object('script/glue.json')
response = json.loads(obj.get()['Body'].read().decode('utf-8'))
tags = response['tags']
drop_fields = response['drop_fields']

f_tags = []
for tag in tags:
    f_tags += tags[tag]


# Filter tag which in the tag list
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["{}/input".format(args['connection_options'])], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

drop_fields_node = DropFields.apply(
    frame=S3bucket_node1,
    paths=drop_fields,
    transformation_ctx="DropFields_node",
)

for tag in tags:
    filter_node = Filter.apply(
        frame=drop_fields_node,
        f=lambda row: (True if row["UsageType"] in tags[tag] else False),
        transformation_ctx="Filter_{}".format(tag),
    )
    print("Filter_{}, count: {}".format(tag, filter_node.count()))
    if filter_node.count() > 0:
        glueContext.write_dynamic_frame.from_options(
            frame=filter_node.coalesce(1),
            connection_type="s3",
            connection_options={
                "path": "{}/output/{}".format(args['connection_options'], tag)},
            format="csv",
            transformation_ctx="AmazonS3_node_{}".format(tag),
        )

other_filter_node = Filter.apply(
    frame=drop_fields_node,
    f=lambda row: (True if row["UsageType"] not in f_tags else False),
    transformation_ctx="Filter_OTHERS",
)
print("Filter_OTHERS, count: {}".format(other_filter_node.count()))
if other_filter_node.count() > 0:
    glueContext.write_dynamic_frame.from_options(
        frame=other_filter_node.coalesce(1),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "{}/output/OTHER/".format(args['connection_options']),
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_node_OTHER",
    )

job.commit()
