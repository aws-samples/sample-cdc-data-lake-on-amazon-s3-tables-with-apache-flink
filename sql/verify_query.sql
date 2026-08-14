SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE CATALOG iceberg WITH (
  'type'='iceberg','catalog-type'='rest','uri'='http://iceberg-rest:8181',
  'warehouse'='s3://warehouse/','io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000','s3.path-style-access'='true','client.region'='us-east-1',
  's3.access-key-id'='minioadmin','s3.secret-access-key'='minioadmin'
);
SELECT order_id, customer, status, qty FROM iceberg.lakehouse.orders ORDER BY order_id;
