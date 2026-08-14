-- Seed for the self-managed MySQL source. Runs once on first container start.
-- A tiny "inventory" OLTP schema — the kind of table you would never want to
-- run analytics against directly in production.

CREATE TABLE IF NOT EXISTS inventory.orders (
  order_id    BIGINT      NOT NULL,
  customer    VARCHAR(64) NOT NULL,
  sku         VARCHAR(32) NOT NULL,
  qty         INT         NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  status      VARCHAR(16) NOT NULL,
  updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                          ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id)
);

INSERT INTO inventory.orders (order_id, customer, sku, qty, amount, status) VALUES
  (1001, 'acme',      'WIDGET-A', 3, 29.97, 'NEW'),
  (1002, 'globex',    'WIDGET-B', 1, 14.50, 'NEW'),
  (1003, 'initech',   'GADGET-C', 5, 99.95, 'PAID'),
  (1004, 'umbrella',  'WIDGET-A', 2, 19.98, 'PAID'),
  (1005, 'stark',     'GADGET-D', 1, 49.99, 'SHIPPED');

-- The CDC user needs replication grants to read the binlog.
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
