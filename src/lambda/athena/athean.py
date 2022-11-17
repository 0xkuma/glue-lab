import boto3
import os
import json

bucket_name = os.getenv('bucket_name') or 'ecv-glue-bucket'

athena = boto3.client('athena', region_name='us-west-2')
s3 = boto3.resource('s3', region_name='us-west-2')
obj = s3.Bucket(bucket_name).Object('script/glue.json')
response = json.loads(obj.get()['Body'].read().decode('utf-8'))
k_tags = list(response['tags'].keys()) + ['OTHER']


def create_table(tag):
    print('Create table: glue-{}'.format(tag))
    athena.start_query_execution(
        QueryString='''
                        CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`glue-{}` (
                        `user_project` string,
                        `usagequantity` float,
                        `usagetype` string,
                        `unblendedrate` float,
                        `user_cost_centre` string,
                        `itemdescription` string
                        )
                        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                        WITH SERDEPROPERTIES ('field.delim' = ',')
                        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                        LOCATION 's3://{}/glue/output/{}'
                        TBLPROPERTIES (
                        'classification' = 'csv',
                        'skip.header.line.count'='1'
                        );
                    '''.format(tag, bucket_name, tag),
        QueryExecutionContext={
            'Database': 'default'
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}/athena/temp/'.format(bucket_name)
        }
    )


def query_item(tag):
    print('Query table: glue-{}'.format(tag))
    athena.start_query_execution(
        QueryString='SELECT * FROM "default"."glue-{}"'.format(tag),
        QueryExecutionContext={
            'Database': 'default'
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}/athena/output/{}'.format(bucket_name, tag)
        }
    )


def lambda_handler(event, context):
    for tag in k_tags:
        create_table(tag)
        query_item(tag)


lambda_handler(None, None)
