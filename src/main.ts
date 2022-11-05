import fs from 'fs';
import path from 'path';
import { App, Stack, StackProps } from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

export class MyStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const labelsFile = fs.readFileSync(path.join(__dirname, 'labels.json'), 'utf8');

    new ssm.StringParameter(this, 'labelParameter', {
      parameterName: '/KL/label',
      stringValue: labelsFile,
    });

    const bucket = new s3.Bucket(this, 'glueBucket', {
      bucketName: 'ecv-glue-bucket',
    });

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
        actions: ['logs:*', 'glue:CreateJob'],
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
