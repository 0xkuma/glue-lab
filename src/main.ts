import path from 'path';
import { App, Stack, StackProps } from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';

export class MyStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const lambdaFn = new lambda.Function(this, 'lambdaFn', {
      code: lambda.Code.fromAsset(path.join(__dirname, 'lambda')),
      handler: 'index.handler',
      runtime: lambda.Runtime.NODEJS_16_X,
    });

    const bucket = new s3.Bucket(this, 'glueBucket', {
      bucketName: 'ecv-glue-bucket',
    });

    bucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.LambdaDestination(lambdaFn));

    new s3deploy.BucketDeployment(this, 'DeployGlueScripts', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue-script'))],
      destinationBucket: bucket,
      destinationKeyPrefix: 'script',
    });

    const role = new iam.Role(this, 'Role', {
      roleName: 'GlueRole',
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    role.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['s3:*'],
        resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
      }),
    );
    role.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['logs:*', 'glue:CreateJob', 'ssm:GetParameter'],
        resources: ['*'],
      }),
    );

    const jobCommandProperty: glue.CfnJob.JobCommandProperty = {
      name: 'glueetl',
      pythonVersion: '3',
      scriptLocation: `s3://${bucket.bucketName}/script/glue.py`,
    };
    new glue.CfnJob(this, 'poc-glue', {
      name: 'poc-glue',
      timeout: 300,
      maxRetries: 0,
      glueVersion: '3.0',
      command: jobCommandProperty,
      role: role.roleArn,
      workerType: 'G.1X',
      numberOfWorkers: 2,
      defaultArguments: {
        '--TempDir': `s3://${bucket.bucketName}/temporary/`,
        '--enable-metrics': '',
        '--enable-continuous-cloudwatch-log': 'true',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-spark-ui': 'false',
        '--connection_options': `s3://${bucket.bucketName}`,
        '--bucket_name': bucket.bucketName,
      },
    });
  }
}

// for development, use account/region from cdk cli
const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const app = new App();

new MyStack(app, 'glue-lab-dev', { env: devEnv });

app.synth();
