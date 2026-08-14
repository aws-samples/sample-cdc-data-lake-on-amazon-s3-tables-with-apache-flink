-- Oracle LogMiner prerequisites, run as SYSDBA on first container start.
-- (gvenzl/oracle-free executes *.sql in /container-entrypoint-initdb.d as SYSDBA
-- in the CDB root.)
--
-- ARCHIVELOG mode is what LogMiner reads; enabling it requires a bounce to
-- MOUNT. After the manual bounce the PDB does not reopen on its own on first
-- boot, so we open all PDBs explicitly — otherwise the seed script that runs
-- next finds FREEPDB1 in MOUNTED state and fails.
--
-- Debezium's LogMiner adapter in a CDB+PDB deployment mines from the CDB root,
-- so the mining user must be a COMMON user (C##) created in the root with
-- CONTAINER=ALL grants. A PDB-local user cannot run LogMiner. Grant set is the
-- one documented by Flink CDC 3.6 / Debezium for CDB databases.

ALTER SYSTEM SET db_recovery_file_dest_size = 10G;
ALTER SYSTEM SET enable_goldengate_replication = TRUE;

SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
-- Reopen the PDBs the bounce left mounted; without this the seed script fails.
ALTER PLUGGABLE DATABASE ALL OPEN;

-- Minimal supplemental logging at the DB level; per-table ALL-column logging is
-- added in seed_oracle.sql so UPDATE/DELETE carry the full "before" image.
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Tablespace for the mining user, in the CDB root and in the PDB.
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Common mining user (used by the Flink oracle-cdc connector).
CREATE USER c##cdc IDENTIFIED BY cdcpw
  DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs CONTAINER=ALL;
GRANT CREATE SESSION TO c##cdc CONTAINER=ALL;
GRANT SET CONTAINER TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##cdc CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##cdc CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##cdc CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##cdc CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##cdc CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##cdc CONTAINER=ALL;
GRANT LOGMINING TO c##cdc CONTAINER=ALL;
GRANT ANALYZE ANY TO c##cdc CONTAINER=ALL;
GRANT CREATE TABLE TO c##cdc CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##cdc CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##cdc CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##cdc CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##cdc CONTAINER=ALL;

-- ---------------------------------------------------------------------------
-- Debezium 1.9.x (bundled in flink-sql-connector-oracle-cdc 3.6.0) banner fix.
-- 23ai Free's two-line BANNER_FULL ("... Release 23.0.0.0.0 - Develop, Learn,
-- and Run for Free\nVersion 23.x.y") matches neither of Debezium's version
-- patterns (one requires "- Production", the other cannot cross the newline),
-- so connection setup dies with "Failed to resolve Oracle database version".
-- Debezium falls back to the legacy single-line BANNER query when BANNER_FULL
-- raises ORA-00904 — so give the mining user a schema-local V$VERSION shadow
-- view exposing only BANNER. Name resolution prefers the user's own schema,
-- Debezium hits ORA-00904 on BANNER_FULL, falls back, and parses cleanly.
GRANT SELECT ON v_$version TO c##cdc CONTAINER=ALL;
CREATE OR REPLACE VIEW c##cdc.v$version AS SELECT banner FROM sys.v_$version;
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE OR REPLACE VIEW c##cdc.v$version AS SELECT banner FROM sys.v_$version;
ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Pre-create Debezium's LGWR flush table in the CDB root. The connector's
-- mining connection opens against the PDB service (see the Flink DDL notes),
-- creates LOG_MINING_FLUSH there, then resets its session to CDB$ROOT for
-- mining — where the flush UPDATE would hit ORA-00942. Having the table in
-- the root (with its seed row) makes the flush path work from either
-- container.
CREATE TABLE c##cdc.log_mining_flush (last_scn NUMBER(19,0));
INSERT INTO c##cdc.log_mining_flush VALUES (0);
COMMIT;
