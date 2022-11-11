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

KEHK_node = Filter.apply(
    frame=drop_fields_node,
    f=lambda row: (bool(True if row["UsageType"] in tags["KEHK"] else False)),
    transformation_ctx="Filter_KEHK",
)
KerryESG_node = Filter.apply(
    frame=drop_fields_node,
    f=lambda row: (bool(True if row["UsageType"]
                   in tags["KerryESG"] else False)),
    transformation_ctx="Filter_KerryESG",
)
KFHK_node = Filter.apply(
    frame=drop_fields_node,
    f=lambda row: (bool(True if row["UsageType"] in tags["KFHK"] else False)),
    transformation_ctx="Filter_KFHK",
)
# KFMS_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KFMS"] else False),
#     transformation_ctx="Filter_KFMS",
# )
# KLHK_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KLHK"] else False),
#     transformation_ctx="Filter_KLHK",
# )
# KLUK_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KLUK"] else False),
#     transformation_ctx="Filter_KLUK",
# )
# KPHARMA_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KPHARMA"] else False),
#     transformation_ctx="Filter_KPHARMA",
# )
# KWHK_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KWHK"] else False),
#     transformation_ctx="Filter_KWHK",
# )
# KWMS_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KWMS"] else False),
#     transformation_ctx="Filter_KWMS",
# )
# KSIS_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KSIS"] else False),
#     transformation_ctx="Filter_KSIS",
# )
# KPHK_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] in tags["KPHK"] else False),
#     transformation_ctx="Filter_KPHK",
# )
# Other tags which not in the list
# OTHER_node = Filter.apply(
#     frame=drop_fields_node,
#     f=lambda row: (True if row["UsageType"] not in f_tags else False),
#     transformation_ctx="Filter_OTHER",
# )

AmazonS3_KEHK = glueContext.write_dynamic_frame.from_options(
    frame=KEHK_node.coalesce(1),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "{}/output/KEHK/".format(args['connection_options']),
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node_KEHK",
)
AmazonS3_KerryESG = glueContext.write_dynamic_frame.from_options(
    frame=KerryESG_node.coalesce(1),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "{}/output/KerryESG/".format(args['connection_options']),
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node_KerryESG",
)
AmazonS3_KFHK = glueContext.write_dynamic_frame.from_options(
    frame=KFHK_node.coalesce(1),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "{}/output/KFHK/".format(args['connection_options']),
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node_KFHK",
)
# glueContext.write_dynamic_frame.from_options(
#     frame=KFMS_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KFMS/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KFMS",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KLHK_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KLHK/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KLHK",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KLUK_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KLUK/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KLUK",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KPHARMA_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KPHARMA/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KPHARMA",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KPHK_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KPHK/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KPHK",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KWHK_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KWHK/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KWHK",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KWMS_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KWMS/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KWMS",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=KSIS_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/KSIS/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_KSIS",
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=OTHER_node.coalesce(1),
#     connection_type="s3",
#     format="csv",
#     connection_options={
#         "path": "{}/output/OTHER/".format(args['connection_options']),
#         "partitionKeys": [],
#     },
#     transformation_ctx="AmazonS3_node_OTHER",
# )

job.commit()
