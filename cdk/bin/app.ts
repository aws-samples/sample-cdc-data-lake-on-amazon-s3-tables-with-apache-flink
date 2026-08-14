#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { ZeroEtlStack } from '../lib/stack';

const app = new App();
// Environment-agnostic on purpose: the stack synthesizes without a concrete
// account/region (VPC uses Fn::GetAZs), so `cdk synth` is reproducible anywhere
// and the committed template contains no account id. Set an explicit `env`
// (account/region) before `cdk deploy` if you want AZ pinning.
new ZeroEtlStack(app, 'ZeroEtlS3TablesFlinkCdc', {
  description:
    'Self-managed MySQL/Postgres -> Flink CDC on MSF -> Apache Iceberg on S3 Tables',
});
