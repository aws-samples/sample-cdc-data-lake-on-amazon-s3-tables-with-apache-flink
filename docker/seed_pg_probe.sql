-- Seed for the PostgreSQL new-table live-pickup probe. One table with rows;
-- a second (customers) is created WHILE the job runs to test scan.newly-added.
CREATE TABLE IF NOT EXISTS public.products (
  sku   TEXT PRIMARY KEY,
  name  TEXT NOT NULL,
  price NUMERIC(10,2) NOT NULL
);
INSERT INTO public.products (sku, name, price) VALUES
  ('WIDGET-A', 'Standard Widget', 9.99),
  ('WIDGET-B', 'Deluxe Widget',  14.50),
  ('GADGET-C', 'Gadget Classic', 19.99);
