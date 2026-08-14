-- The whole pipeline, as Flink SQL. No ETL job to write, schedule, or babysit:
-- a single streaming INSERT keeps an Iceberg table continuously in sync with the
-- source. Run inside the Flink SQL client (see README).

SET 'execution.checkpointing.interval' = '10s';

-- 1) The Iceberg catalog. Locally this points at the iceberg-rest-fixture; in
--    production you change ONLY these properties to the S3 Tables Iceberg REST
--    endpoint (uri = https://s3tables.<region>.amazonaws.com/iceberg,
--    warehouse = the table-bucket ARN, rest.sigv4-enabled = true). The job below
--    is identical.
CREATE CATALOG iceberg WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.path-style-access' = 'true',
  'client.region'        = 'us-east-1',
  's3.access-key-id'     = 'minioadmin',
  's3.secret-access-key' = 'minioadmin'
);

CREATE DATABASE IF NOT EXISTS iceberg.lakehouse;

-- 2) The CDC source. This is a live view of the MySQL table via the binlog:
--    the first read is a consistent snapshot, then it streams every insert,
--    update and delete as a changelog. No Kafka, no Debezium to operate.
CREATE TEMPORARY TABLE orders_src (
  order_id   BIGINT,
  customer   STRING,
  sku        STRING,
  qty        INT,
  amount     DECIMAL(10,2),
  status     STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'      = 'mysql-cdc',
  'hostname'       = 'mysql',
  'port'           = '3306',
  'username'       = 'cdc',
  'password'       = 'cdcpw',
  'database-name'  = 'inventory',
  'table-name'     = 'orders',
  'server-id'      = '5400-5404',
  'scan.incremental.snapshot.enabled' = 'true'
);

-- 3) The Iceberg sink table. Upsert mode keeps it a faithful mirror of the
--    source primary key — updates and deletes are applied, not appended.
CREATE TABLE IF NOT EXISTS iceberg.lakehouse.orders (
  order_id   BIGINT,
  customer   STRING,
  sku        STRING,
  qty        INT,
  amount     DECIMAL(10,2),
  status     STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'format-version'          = '2',
  'write.upsert.enabled'    = 'true'
);

-- 4) Start the continuous sync. This statement returns a job id and keeps
--    running; the table is queryable by any Iceberg reader from now on.
INSERT INTO iceberg.lakehouse.orders SELECT * FROM orders_src;
