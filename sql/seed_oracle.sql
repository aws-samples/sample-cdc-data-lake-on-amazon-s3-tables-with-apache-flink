-- Seed for the self-managed Oracle source (gvenzl/oracle-free, 23ai).
-- Runs as SYSDBA in the CDB root (gvenzl entrypoint), so switch into the
-- FREEPDB1 pluggable database first; the table lives in the `cdc` app schema
-- that the image creates (APP_USER=cdc). Same "inventory" orders shape.
--
-- Flink CDC (oracle-cdc) reads redo via LogMiner, which requires:
--   * the database in ARCHIVELOG mode                (enabled in 01-setup.sql)
--   * minimal supplemental logging at the DB level   (enabled in 01-setup.sql)
--   * per-table supplemental logging of ALL columns  (below) so UPDATE/DELETE
--     changelog rows carry the full "before" image
--   * a COMMON mining user (c##cdc) with the documented LogMiner grants
--     (created in 01-setup.sql)

ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE TABLE cdc.inventory_orders (
  order_id   NUMBER(19)    PRIMARY KEY,
  customer   VARCHAR2(64)  NOT NULL,
  sku        VARCHAR2(32)  NOT NULL,
  qty        NUMBER(10)    NOT NULL,
  amount     NUMBER(10,2)  NOT NULL,
  status     VARCHAR2(16)  NOT NULL,
  updated_at TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL
);
ALTER TABLE cdc.inventory_orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO cdc.inventory_orders VALUES (1001,'acme','WIDGET-A',3,29.97,'NEW',CURRENT_TIMESTAMP);
INSERT INTO cdc.inventory_orders VALUES (1002,'globex','WIDGET-B',1,14.50,'NEW',CURRENT_TIMESTAMP);
INSERT INTO cdc.inventory_orders VALUES (1003,'initech','GADGET-C',5,99.95,'PAID',CURRENT_TIMESTAMP);
INSERT INTO cdc.inventory_orders VALUES (1004,'umbrella','WIDGET-A',2,19.98,'PAID',CURRENT_TIMESTAMP);
INSERT INTO cdc.inventory_orders VALUES (1005,'stark','GADGET-D',1,49.99,'SHIPPED',CURRENT_TIMESTAMP);
COMMIT;
