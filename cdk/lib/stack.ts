import * as fs from 'fs';
import * as path from 'path';
import { Stack, StackProps, CfnResource, RemovalPolicy, Duration, CfnOutput } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as msf from 'aws-cdk-lib/aws-kinesisanalyticsv2';

/**
 * Real-AWS Tier-B architecture for the blog:
 *   self-managed MySQL/Postgres  ->  Flink CDC on Managed Service for Apache
 *   Flink (MSF, Flink 2.3, packaged JAR)  ->  Apache Iceberg on Amazon S3 Tables
 *
 * MSF runs a bundled DataStream/Table-API JAR (NOT the Flink CDC YAML pipeline
 * CLI — that runtime is not an MSF entrypoint). The JAR uses the Iceberg
 * RESTCatalog pointed at the S3 Tables Iceberg REST endpoint with SigV4.
 *
 * `cdk synth` only — deploy is a hard, user-approved gate.
 */
export class ZeroEtlStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // --- S3 Tables: the Iceberg lakehouse destination -----------------------
    // Escape hatch (CfnResource) keeps this synth-able across CDK versions
    // regardless of whether the typed aws-s3tables L1 is present.
    // Name is context-overridable (-c tableBucketName=...) because S3 Tables
    // tombstones a deleted bucket name for a while — a rollback + fast retry
    // under the same name fails with a 409 "transitional state".
    const tableBucketName =
      (this.node.tryGetContext('tableBucketName') as string) || 'zero-etl-lakehouse';
    const tableBucket = new CfnResource(this, 'LakehouseTableBucket', {
      type: 'AWS::S3Tables::TableBucket',
      properties: { TableBucketName: tableBucketName },
    });
    const tableBucketArn = tableBucket.getAtt('TableBucketARN').toString();

    // Single-level namespace (S3 Tables supports one level only).
    const namespace = new CfnResource(this, 'LakehouseNamespace', {
      type: 'AWS::S3Tables::Namespace',
      properties: { TableBucketARN: tableBucketArn, Namespace: 'lakehouse' },
    });
    namespace.addDependency(tableBucket);

    // --- MSF application JAR, uploaded as a CDK asset at deploy time --------
    // (Build it first: `mvn -f ../app/pom.xml package`.) Using an asset avoids
    // the first-deploy chicken-and-egg of referencing a manually-uploaded key
    // in a bucket this same stack creates.
    const appJar = new s3assets.Asset(this, 'AppJar', {
      path: path.join(__dirname, '..', '..', 'app', 'target', 'cdc-to-iceberg-1.0.jar'),
    });

    // --- VPC so MSF can reach a self-managed source DB ----------------------
    const vpc = new ec2.Vpc(this, 'Vpc', { maxAzs: 2, natGateways: 1 });
    const appSg = new ec2.SecurityGroup(this, 'MsfSg', {
      vpc,
      description: 'MSF app ENIs; egress to self-managed DB + S3 Tables',
      allowAllOutbound: true,
    });

    // --- IAM role for the MSF application -----------------------------------
    const role = new iam.Role(this, 'MsfRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
    });
    appJar.grantRead(role);
    // S3 Tables data-plane + catalog permissions, scoped to this table bucket.
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3tables:GetTableBucket', 's3tables:ListNamespaces',
        's3tables:GetNamespace', 's3tables:CreateNamespace',
        's3tables:ListTables', 's3tables:GetTable', 's3tables:CreateTable',
        's3tables:GetTableData', 's3tables:PutTableData',
        's3tables:GetTableMetadataLocation', 's3tables:UpdateTableMetadataLocation',
      ],
      resources: [tableBucketArn, `${tableBucketArn}/table/*`],
    }));
    // ENI management for VPC connectivity (the documented MSF VPC policy —
    // DescribeDhcpOptions is required; omitting it fails app creation with
    // "service does not have the necessary privileges to configure VPC connectivity").
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'ec2:CreateNetworkInterface', 'ec2:DescribeNetworkInterfaces',
        'ec2:DeleteNetworkInterface', 'ec2:CreateNetworkInterfacePermission',
        'ec2:DescribeVpcs', 'ec2:DescribeSubnets', 'ec2:DescribeSecurityGroups',
        'ec2:DescribeDhcpOptions',
      ],
      resources: ['*'],
    }));

    const logGroup = new logs.LogGroup(this, 'MsfLogs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const logStream = new logs.LogStream(this, 'MsfLogStream', { logGroup });
    // CloudWatch Logs permissions for the service role. Without these the
    // application starts (and fails) SILENTLY — the configured log stream
    // stays empty and there is no stack trace to debug from. The documented
    // MSF logging policy needs DescribeLogGroups/Streams account-wide plus
    // PutLogEvents on the configured stream.
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:DescribeLogGroups'],
      resources: [`arn:aws:logs:${this.region}:${this.account}:log-group:*`],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:DescribeLogStreams'],
      resources: [`${logGroup.logGroupArn}:*`],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:PutLogEvents'],
      resources: [
        `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`,
      ],
    }));

    // ======================================================================
    // OPTIONAL, TEST-ONLY: a throwaway self-managed source database in the VPC.
    // Enable with:  cdk deploy -c testSource=mysql|postgres|oracle
    //   (-c withTestSource=true is kept as a back-compat alias for mysql)
    // Absent by default -> the stack is byte-identical (zero extra resources).
    //
    // This exists ONLY so a reader without an existing database can try the
    // walkthrough end to end. Each engine runs the SAME container setup as the
    // local docker-compose variants, seeded with the identical `inventory.orders`
    // demo table and a `cdc` user with the CDC privileges that engine needs. The
    // demo credentials are intentional and never leave the VPC: the instance has
    // no public IP, no SSH keypair, no open management port — you manage it via
    // SSM Session Manager only, and its security group accepts ONLY the engine's
    // database port and ONLY from the MSF application's security group.
    //
    // In production you point the MSF app at YOUR real self-managed database
    // instead; omit the flag and this construct never exists.
    //
    // Resolved before the MSF app so, when enabled, the app's `cdc` property
    // group can point at this instance's private DNS (see below).
    // ======================================================================
    const testSourceCtx = this.node.tryGetContext('testSource');
    const withTestSourceAlias =
      this.node.tryGetContext('withTestSource') === true ||
      this.node.tryGetContext('withTestSource') === 'true';
    // withTestSource=true is the legacy mysql alias.
    const engine: string | undefined =
      (typeof testSourceCtx === 'string' && testSourceCtx) ||
      (withTestSourceAlias ? 'mysql' : undefined);

    // Captured from the test-source block below so the MSF `cdc` property group
    // can be wired to the provisioned instance. Undefined when no flag is set.
    let testDbHost: string | undefined;
    let testDbPort: number | undefined;
    let testDbDatabase: string | undefined;
    let testDbTable: string | undefined;

    if (engine) {
      const allowed = ['mysql', 'postgres', 'oracle'];
      if (!allowed.includes(engine)) {
        throw new Error(`testSource must be one of ${allowed.join('|')} (got "${engine}")`);
      }

      // Per-engine knobs. Oracle Free (23ai) needs materially more memory and
      // CPU than MySQL/Postgres (the LogMiner + archivelog + PGA/SGA footprint
      // does not fit a 2 GiB t4g.small), so it gets t4g.large; the lightweight
      // engines stay on the cheapest sensible current-gen t4g.small.
      const enginePort: Record<string, number> = { mysql: 3306, postgres: 5432, oracle: 1521 };
      const engineSize: Record<string, ec2.InstanceSize> = {
        mysql: ec2.InstanceSize.SMALL,
        // medium: t4g.small capacity proved scarce across BOTH eu-central-1
        // AZs on consecutive attempts (each error recommending the other
        // AZ) -- the medium pool is deeper and PG is light anyway.
        postgres: ec2.InstanceSize.MEDIUM,
        oracle: ec2.InstanceSize.LARGE,
      };
      // The demo table/schema each container is seeded with (see userData
      // below): MySQL uses `inventory.orders`; Postgres/Oracle seed
      // `inventory_orders` (Oracle inside the FREEPDB1 pluggable database).
      const engineDatabase: Record<string, string> = { mysql: 'inventory', postgres: 'inventory', oracle: 'FREEPDB1' };
      const engineTable: Record<string, string> = { mysql: 'orders', postgres: 'inventory_orders', oracle: 'inventory_orders' };
      const port = enginePort[engine];

      const dbSg = new ec2.SecurityGroup(this, 'TestSourceDbSg', {
        vpc,
        description: `TEST-ONLY self-managed ${engine}; ${port} from MSF SG only`,
        allowAllOutbound: true, // egress via existing NAT to pull the image + reach SSM
      });
      dbSg.connections.allowFrom(appSg, ec2.Port.tcp(port), `MSF Flink CDC to ${engine}`);

      // Minimal instance role: SSM Session Manager access only (no SSH).
      const dbRole = new iam.Role(this, 'TestSourceDbRole', {
        assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
        ],
      });

      const userData = ec2.UserData.forLinux();
      userData.addCommands('set -eux', 'dnf install -y docker', 'systemctl enable --now docker', 'mkdir -p /opt/cdc');

      if (engine === 'mysql') {
        userData.addCommands(
          "cat > /opt/cdc/seed.sql <<'SEED'",
          'CREATE TABLE IF NOT EXISTS inventory.orders (',
          '  order_id BIGINT NOT NULL, customer VARCHAR(64) NOT NULL, sku VARCHAR(32) NOT NULL,',
          '  qty INT NOT NULL, amount DECIMAL(10,2) NOT NULL, status VARCHAR(16) NOT NULL,',
          '  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,',
          '  PRIMARY KEY (order_id));',
          "INSERT INTO inventory.orders (order_id, customer, sku, qty, amount, status) VALUES",
          "  (1001,'acme','WIDGET-A',3,29.97,'NEW'),(1002,'globex','WIDGET-B',1,14.50,'NEW'),",
          "  (1003,'initech','GADGET-C',5,99.95,'PAID'),(1004,'umbrella','WIDGET-A',2,19.98,'PAID'),",
          "  (1005,'stark','GADGET-D',1,49.99,'SHIPPED');",
          "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';",
          'FLUSH PRIVILEGES;',
          'SEED',
          'docker run -d --name cdc-mysql --restart unless-stopped \\',
          '  -e MYSQL_ROOT_PASSWORD=rootpw -e MYSQL_DATABASE=inventory -e MYSQL_USER=cdc -e MYSQL_PASSWORD=cdcpw \\',
          '  -p 3306:3306 -v /opt/cdc/seed.sql:/docker-entrypoint-initdb.d/seed.sql:ro \\',
          '  mysql:8.4.11 --server-id=223344 --log-bin=mysql-bin --binlog-format=ROW \\',
          '  --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON',
        );
      } else if (engine === 'postgres') {
        userData.addCommands(
          // postgres logical decoding: wal_level=logical + a role WITH REPLICATION.
          "cat > /opt/cdc/seed.sql <<'SEED'",
          'CREATE TABLE inventory_orders (',
          '  order_id BIGINT PRIMARY KEY, customer VARCHAR(64) NOT NULL, sku VARCHAR(32) NOT NULL,',
          '  qty INT NOT NULL, amount NUMERIC(10,2) NOT NULL, status VARCHAR(16) NOT NULL,',
          '  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);',
          "INSERT INTO inventory_orders (order_id, customer, sku, qty, amount, status) VALUES",
          "  (1001,'acme','WIDGET-A',3,29.97,'NEW'),(1002,'globex','WIDGET-B',1,14.50,'NEW'),",
          "  (1003,'initech','GADGET-C',5,99.95,'PAID'),(1004,'umbrella','WIDGET-A',2,19.98,'PAID'),",
          "  (1005,'stark','GADGET-D',1,49.99,'SHIPPED');",
          // The bootstrap superuser is `cdc`; grant it REPLICATION for logical slots.
          "ALTER ROLE cdc WITH REPLICATION;",
          'SEED',
          'docker run -d --name cdc-postgres --restart unless-stopped \\',
          '  -e POSTGRES_DB=inventory -e POSTGRES_USER=cdc -e POSTGRES_PASSWORD=cdcpw \\',
          '  -p 5432:5432 -v /opt/cdc/seed.sql:/docker-entrypoint-initdb.d/seed.sql:ro \\',
          '  postgres:18 -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10',
        );
      } else {
        // oracle — gvenzl/oracle-free (23ai). LogMiner CDC needs ARCHIVELOG mode +
        // supplemental logging + a user with mining privileges. gvenzl exposes a
        // setup hook that runs as SYSDBA on first start; we enable archivelog
        // (restart to mount, alter, open), turn on min supplemental logging, and
        // grant the documented LogMiner privilege set to the `cdc` app user.
        // LogMiner CDC prerequisites are NON-TRIVIAL (ARCHIVELOG bounce +
        // PDB reopen, common c##cdc mining user with CONTAINER=ALL grants,
        // Debezium 1.9 banner-parse shadow view, LGWR flush table in
        // CDB$ROOT). Those were debugged and verified in
        // sql/oracle-setup.sql + sql/seed_oracle.sql -- read them at synth
        // time so the CDK test source and the local harness can never drift
        // (an earlier inline copy here silently lacked the fixes). Image tag
        // is PINNED: the floating :23-slim tag was repointed to a rebranded
        // release whose banner Debezium 1.9.8 cannot parse.
        const oracleSetupSql = fs.readFileSync(
          path.join(__dirname, '..', '..', 'sql', 'oracle-setup.sql'), 'utf8');
        const oracleSeedSql = fs.readFileSync(
          path.join(__dirname, '..', '..', 'sql', 'seed_oracle.sql'), 'utf8');
        userData.addCommands(
          "cat > /opt/cdc/setup.sql <<'ORASETUP'",
          oracleSetupSql,
          'ORASETUP',
          "cat > /opt/cdc/seed.sql <<'ORASEED'",
          oracleSeedSql,
          'ORASEED',
          'docker run -d --name cdc-oracle --restart unless-stopped \\',
          '  -e ORACLE_PASSWORD=cdcpw -e APP_USER=cdc -e APP_USER_PASSWORD=cdcpw \\',
          '  -p 1521:1521 \\',
          '  -v /opt/cdc/setup.sql:/container-entrypoint-initdb.d/01-setup.sql:ro \\',
          '  -v /opt/cdc/seed.sql:/container-entrypoint-initdb.d/02-seed.sql:ro \\',
          '  gvenzl/oracle-free:23.9-slim',
        );
      }

      const dbInstance = new ec2.Instance(this, 'TestSourceDb', {
        vpc,
        // t4g capacity comes and goes per AZ (hit empirically in BOTH 1a and
        // 1b on consecutive days) -- make the subnet pick context-overridable:
        //   -c testSourceAz=0|1   (index into vpc.availabilityZones)
        // Any single private subnet works; the MSF app reaches it across AZs.
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, // no public IP
          availabilityZones: [
            vpc.availabilityZones[
              Number(this.node.tryGetContext('testSourceAz') ?? 1)
            ],
          ],
        },
        instanceType: ec2.InstanceType.of(ec2.InstanceClass.T4G, engineSize[engine]),
        machineImage: ec2.MachineImage.latestAmazonLinux2023({
          cpuType: ec2.AmazonLinuxCpuType.ARM_64,
        }),
        // User data only runs on FIRST boot. Without this flag, switching
        // -c testSource engines does a stop-modify-start on the same
        // instance, cloud-init never re-runs, and the OLD engine's
        // container keeps serving (empirically hit on the mysql->postgres
        // switch: the box kept running cdc-mysql, PG connection refused).
        userDataCausesReplacement: true,
        securityGroup: dbSg,
        role: dbRole,
        userData,
        requireImdsv2: true,
        // No keyName -> no SSH keypair. No SSH ingress rule anywhere -> no port 22.
      });

      new CfnOutput(this, 'TestSourceDbHost', {
        value: dbInstance.instancePrivateDnsName,
        description: `TEST-ONLY ${engine} private DNS — set as the CDC hostname in your MSF app properties`,
      });
      new CfnOutput(this, 'TestSourceEngine', { value: engine });

      // Hand the provisioned instance's coordinates to the MSF `cdc` property
      // group below so the app targets it out of the box.
      testDbHost = dbInstance.instancePrivateDnsName;
      testDbPort = port;
      testDbDatabase = engineDatabase[engine];
      testDbTable = engineTable[engine];
    }

    // --- CDC source connection for the MSF app JAR --------------------------
    // The packaged app (app/src/main/java/com/example/cdc/CdcToIcebergJob.java)
    // reads these from the "cdc" environment property group. When a test source
    // is provisioned above, wire them to that in-VPC instance and the exact
    // credentials/table its container was seeded with. Otherwise read from CDK
    // context so you can point the app at your OWN self-managed database:
    //   cdk synth -c cdcHostname=db.internal -c cdcPort=3306 \
    //             -c cdcDatabase=inventory   -c cdcTable=orders \
    //             -c cdcUsername=cdc          -c cdcPassword=...
    // With neither a flag nor context, clearly-named placeholders keep synth
    // deterministic (you must set real values before deploy).
    const cdcCtx = (key: string, fallback: string): string => {
      const v = this.node.tryGetContext(key);
      return typeof v === 'string' && v ? v : fallback;
    };
    const cdcPropertyMap: Record<string, string> = engine
      ? {
          engine: engine,
          hostname: testDbHost!,
          port: String(testDbPort!),
          'database-name': testDbDatabase!,
          'table-name': testDbTable!,
          // Oracle LogMiner requires the COMMON mining user (c##cdc) created
          // in the CDB root -- the PDB-local app user cannot run LogMiner.
          username: engine === 'oracle' ? 'c##cdc' : 'cdc',
          password: 'cdcpw',
        }
      : {
          engine: cdcCtx('cdcEngine', 'mysql'),
          hostname: cdcCtx('cdcHostname', 'REPLACE_WITH_YOUR_DB_HOSTNAME'),
          port: cdcCtx('cdcPort', '3306'),
          'database-name': cdcCtx('cdcDatabase', 'inventory'),
          'table-name': cdcCtx('cdcTable', 'orders'),
          username: cdcCtx('cdcUsername', 'cdc'),
          password: cdcCtx('cdcPassword', 'REPLACE_WITH_YOUR_DB_PASSWORD'),
        };
    // Sync mode: 'single' (Table API, one table — the post's main path) or
    // 'dynamic' (DataStream + DynamicIcebergSink, whole schema with
    // create-tables-on-the-fly). Selectable per deploy: -c cdcMode=dynamic
    cdcPropertyMap['mode'] = cdcCtx('cdcMode', 'single');
    // Newly-added-table semantics for dynamic mode (verified empirically):
    // 'false' (default) = LIVE pickup of tables created while the job runs
    // (binlog-only, complete since a new table is empty at creation);
    // 'true' = live pickup off, savepoint restart re-discovers newly matched
    // tables WITH a snapshot backfill (use when adding pre-existing tables).
    cdcPropertyMap['scan.newly-added-table.enabled'] =
      cdcCtx('cdcScanNewlyAdded', 'false');

    // --- Managed Service for Apache Flink application (Flink 2.3) -----------
    const app = new msf.CfnApplication(this, 'CdcApp', {
      runtimeEnvironment: 'FLINK-2_3',
      serviceExecutionRole: role.roleArn,
      // Context-overridable: changing the name REPLACES the application, which
      // is the remedy for a poisoned job-result store (a failed cross-mode
      // savepoint restore leaves a dirty terminal entry under the app's pinned
      // job id that survives force stop — a fresh application starts clean).
      applicationName: cdcCtx('msfAppName', 'zero-etl-cdc-to-s3tables'),
      applicationConfiguration: {
        // Snapshots (savepoints) let a stop/start resume the binlog position —
        // and they are what makes scan.newly-added-table.enabled pick up
        // tables created after the job first started.
        applicationSnapshotConfiguration: { snapshotsEnabled: true },
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: `arn:aws:s3:::${appJar.s3BucketName}`,
              fileKey: appJar.s3ObjectKey,
            },
          },
          codeContentType: 'ZIPFILE',
        },
        flinkApplicationConfiguration: {
          checkpointConfiguration: {
            configurationType: 'CUSTOM',
            checkpointingEnabled: true,
            checkpointInterval: 10000,
            minPauseBetweenCheckpoints: 5000,
          },
          parallelismConfiguration: {
            configurationType: 'CUSTOM',
            parallelism: 2,
            parallelismPerKpu: 1,
            autoScalingEnabled: true,
          },
        },
        environmentProperties: {
          propertyGroups: [
            {
              propertyGroupId: 'iceberg',
              propertyMap: {
                'catalog.uri': `https://s3tables.${this.region}.amazonaws.com/iceberg`,
                'catalog.warehouse': tableBucketArn,
                'catalog.rest.sigv4-enabled': 'true',
                'catalog.rest.signing-name': 's3tables',
                // Context-overridable (-c icebergNamespace=...). Give dynamic
                // mode its OWN namespace: its JSON-inference schema types
                // (e.g. decimal -> string) collide with tables the Table API
                // mode created (Cannot change column type) if they share one.
                'catalog.namespace': cdcCtx('icebergNamespace', 'lakehouse'),
                // Iceberg table format version. Default v2: readable today by
                // Athena, Redshift, Spark, Trino, and Flink. S3 Tables also
                // supports v3 (deletion vectors + Variant) -- opt in with
                // -c formatVersion=3 once every query engine you use reads
                // v3; the upgrade is one-way.
                'catalog.format-version': cdcCtx('formatVersion', '2'),
              },
            },
            {
              propertyGroupId: 'cdc',
              propertyMap: cdcPropertyMap,
            },
          ],
        },
        vpcConfigurations: [{
          subnetIds: vpc.privateSubnets.map((s) => s.subnetId),
          securityGroupIds: [appSg.securityGroupId],
        }],
      },
    });
    app.node.addDependency(namespace);

    const msfLogging = new msf.CfnApplicationCloudWatchLoggingOption(this, 'MsfLogging', {
      applicationName: app.applicationName!,
      cloudWatchLoggingOption: {
        logStreamArn: `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`,
      },
    });
    // applicationName is a plain string, so CloudFormation infers no ordering —
    // add it explicitly, or the logging option races ahead of the app and fails
    // with "Application ... does not exist".
    msfLogging.node.addDependency(app);

    new CfnOutput(this, 'TableBucketArn', { value: tableBucketArn });
    new CfnOutput(this, 'AppJarS3Uri', { value: appJar.s3ObjectUrl });
    new CfnOutput(this, 'MsfApplicationName', { value: app.applicationName! });
  }
}
