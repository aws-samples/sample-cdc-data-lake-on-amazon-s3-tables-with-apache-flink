-- Seed for the self-managed PostgreSQL source. Runs once on first container
-- start. Same "inventory" orders table as the MySQL variant.
--
-- Logical decoding prerequisites for Flink CDC (postgres-cdc):
--   * the server runs with wal_level=logical (set on the container command line)
--   * the replication user has the REPLICATION attribute
--   * the table has REPLICA IDENTITY FULL so UPDATE/DELETE changelog rows carry
--     the full "before" image (otherwise only the PK is emitted)

CREATE TABLE orders (
  order_id   BIGINT        PRIMARY KEY,
  customer   VARCHAR(64)   NOT NULL,
  sku        VARCHAR(32)   NOT NULL,
  qty        INT           NOT NULL,
  amount     NUMERIC(10,2) NOT NULL,
  status     VARCHAR(16)   NOT NULL,
  updated_at TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders (order_id, customer, sku, qty, amount, status) VALUES
  (1001, 'acme',     'WIDGET-A', 3, 29.97, 'NEW'),
  (1002, 'globex',   'WIDGET-B', 1, 14.50, 'NEW'),
  (1003, 'initech',  'GADGET-C', 5, 99.95, 'PAID'),
  (1004, 'umbrella', 'WIDGET-A', 2, 19.98, 'PAID'),
  (1005, 'stark',    'GADGET-D', 1, 49.99, 'SHIPPED');

-- POSTGRES_USER=cdc is the bootstrap superuser (already has REPLICATION), but be
-- explicit so the intent is clear when readers adapt this to a non-superuser.
ALTER ROLE cdc WITH REPLICATION;
