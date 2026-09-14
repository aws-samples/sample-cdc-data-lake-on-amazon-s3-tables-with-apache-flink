-- Seed for the PostgreSQL dynamic-mode harness (docker-compose.pgdynamic.yml).
-- Two ordinary tables prove snapshot; tables created later prove live pickup.
CREATE TABLE IF NOT EXISTS public.orders (
  order_id   BIGINT PRIMARY KEY,
  customer   VARCHAR(64),
  status     VARCHAR(16),
  qty        INT,
  updated_at TIMESTAMP(3)
);
INSERT INTO public.orders VALUES
  (1001, 'acme', 'NEW', 1, now()),
  (1002, 'globex', 'NEW', 2, now());

CREATE TABLE IF NOT EXISTS public.products (
  sku   VARCHAR(32) PRIMARY KEY,
  name  VARCHAR(64),
  price DECIMAL(8,2)
);
INSERT INTO public.products VALUES
  ('SKU-1', 'widget', 9.99),
  ('SKU-2', 'gadget', 19.99);

-- Heartbeat table: lives INSIDE the captured schema so its updates advance
-- the replication slot (see the README's replication-slot retention section).
-- The job drops its records before the sink via cdc.heartbeat-table.
CREATE TABLE IF NOT EXISTS public.cdc_heartbeat (
  id INT PRIMARY KEY,
  beat TIMESTAMP(3)
);
INSERT INTO public.cdc_heartbeat VALUES (1, now());
