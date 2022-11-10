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


def is_value_in_list(value, tag):
    print("checking value: {}, tag: {}".format(value, tags))
    for item in tags[tag]:
        if value == item:
            return True
    return False


print("Start drop fields")
drop_fields_node = DropFields.apply(
    frame=S3bucket_node1,
    paths=drop_fields,
    transformation_ctx="DropFields_node",
)

print("Start filter tags")
for tag in tags:
    if len(tags[tag]) == 0:
        continue
    print("Start filter tag: {}".format(tag))
    node = Filter.apply(
        frame=drop_fields_node,
        f=lambda row: (is_value_in_list(row["UsageType"], tag)),
        transformation_ctx="Filter_{}".format(tag),
    )
    glueContext.write_dynamic_frame.from_options(
        frame=node.coalesce(1),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "{}/output/{}/".format(args['connection_options'], tag),
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_node_{}".format(tag),
    )

print("Start filter other tag")
# Other tags which not in the list
other_node = Filter.apply(
    frame=drop_fields_node,
    f=lambda row: (True if row["UsageType"]
                   not in f_tags or len(f_tags) == 0 else False),
    transformation_ctx="Filter_other",
)
glueContext.write_dynamic_frame.from_options(
    frame=other_node.coalesce(1),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "{}/output/{}/".format(args['connection_options'], "other"),
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node_other",
)

job.commit()
