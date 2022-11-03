import { App, Stack, StackProps } from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export class MyStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const role = new iam.Role(this, 'Role', {
      roleName: 'GlueRole',
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    role.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [],
        resources: ['*'],
      }),
    );

    const jobCommandProperty: glue.CfnJob.JobCommandProperty = {
      name: 'poc-glue',
      pythonVersion: '3',
      scriptLocation: '',
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
        '--TempDir': '',
        '--enable-metrics': '',
        '--enable-continuous-cloudwatch-log': 'true',
        '--enable-continuous-log-filter': 'true',
        '--job-bookmark-option': 'job-bookmark-disable',
        '---enable-spark-ui': 'false',
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
