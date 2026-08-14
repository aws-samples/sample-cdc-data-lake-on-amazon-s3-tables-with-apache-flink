-- Oracle variant of the pipeline. Identical to cdc_to_iceberg.sql except the
-- source connector is `oracle-cdc` (redo mining via LogMiner). The catalog and
-- sink are unchanged. Run inside the Flink SQL client (see README).
--
-- CDB + PDB model (Oracle 12c+): `database-name` is the CDB service (FREE on
-- gvenzl/oracle-free) and the PDB is passed via `debezium.database.pdb.name`.
-- LogMiner mines from the CDB root, so the connector authenticates as the
-- COMMON user c##cdc created in sql/oracle-setup.sql — a PDB-local user cannot
-- mine. Oracle names are upper-cased by default: the app schema is CDC and the
-- table is INVENTORY_ORDERS.

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

-- Oracle CDC source via LogMiner. `online_catalog` mining strategy avoids
-- writing the dictionary to redo (fast, low overhead) and is the recommended
-- default for a single-schema demo. Config notes for CDB+PDB (23ai Free):
--  * The JDBC `url` targets the PDB service (FREEPDB1) so the connector's
--    table discovery and snapshot reads see the application table (ALL_TABLES
--    in the CDB root cannot see PDB objects).
--  * `debezium.database.pdb.name` stays set: Debezium then resets the mining
--    session to CDB$ROOT before calling DBMS_LOGMNR (which raises ORA-65040
--    if invoked inside a PDB).
--  * `database-name` matches the PDB so snapshot-phase TableIds and redo-event
--    TableIds agree — the incremental source's snapshot->stream schema handoff
--    breaks if they differ.
--  * CONTINUOUS_MINE was removed in Oracle 19c+; do not set
--    `debezium.log.mining.continuous.mine` against 21c/23ai.
--  * The mining user is the COMMON user c##cdc (CONTAINER=ALL grants).
CREATE TEMPORARY TABLE orders_src (
  ORDER_ID   BIGINT,
  CUSTOMER   STRING,
  SKU        STRING,
  QTY        INT,
  AMOUNT     DECIMAL(10,2),
  STATUS     STRING,
  UPDATED_AT TIMESTAMP(3),
  PRIMARY KEY (ORDER_ID) NOT ENFORCED
) WITH (
  'connector'     = 'oracle-cdc',
  'hostname'      = 'oracle',
  'port'          = '1521',
  'username'      = 'c##cdc',
  'password'      = 'cdcpw',
  'url'           = 'jdbc:oracle:thin:@oracle:1521/FREEPDB1',
  'database-name' = 'FREEPDB1',
  'schema-name'   = 'CDC',
  'table-name'    = 'INVENTORY_ORDERS',
  'debezium.database.pdb.name'   = 'FREEPDB1',
  'debezium.log.mining.strategy' = 'online_catalog'
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
