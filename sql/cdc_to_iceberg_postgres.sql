-- PostgreSQL variant of the pipeline. Identical to cdc_to_iceberg.sql except the
-- source connector is `postgres-cdc` (logical decoding via pgoutput). The
-- catalog and sink are byte-for-byte the same — only the source WITH clause
-- changes when you swap engines. Run inside the Flink SQL client (see README).

SET 'execution.checkpointing.interval' = '10s';

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

-- PostgreSQL CDC source. The first read is a consistent snapshot, then the
-- connector streams the WAL through a logical replication slot. `pgoutput` is
-- the in-core output plugin (no wal2json/decoderbufs install needed), and
-- `slot.name` is the durable replication slot Postgres creates for this job.
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
  'connector'           = 'postgres-cdc',
  'hostname'            = 'postgres',
  'port'                = '5432',
  'username'            = 'cdc',
  'password'            = 'cdcpw',
  'database-name'       = 'inventory',
  'schema-name'         = 'public',
  'table-name'          = 'orders',
  'slot.name'           = 'flink_orders',
  'decoding.plugin.name'              = 'pgoutput',
  'scan.incremental.snapshot.enabled' = 'true'
);

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
  'format-version'       = '2',
  'write.upsert.enabled' = 'true'
);

INSERT INTO iceberg.lakehouse.orders SELECT * FROM orders_src;
