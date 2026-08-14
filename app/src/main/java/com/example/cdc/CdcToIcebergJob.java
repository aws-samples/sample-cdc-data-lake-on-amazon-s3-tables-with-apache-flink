package com.example.cdc;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Managed Service for Apache Flink entrypoint for the blog's Tier-B path:
 *
 *   self-managed MySQL  ->  Flink CDC (mysql-cdc)  ->  Apache Iceberg REST
 *   catalog on Amazon S3 Tables (SigV4).
 *
 * This is the SAME pipeline as {@code sql/cdc_to_iceberg.sql}. The only thing
 * that changes between laptop and cloud is the catalog's connection
 * properties, which here come from the MSF application's environment property
 * groups instead of being hard-coded in the SQL file:
 *
 *   group "iceberg" : catalog.uri, catalog.warehouse, catalog.rest.sigv4-enabled,
 *                     catalog.rest.signing-name, catalog.namespace,
 *                     catalog.format-version
 *   group "cdc"     : hostname, port, database-name, table-name, username, password
 *
 * Every property is required. A missing key fails fast at startup with a
 * message naming the exact group + key, so a misconfigured MSF app surfaces the
 * problem immediately in the CloudWatch logs rather than deep inside a catalog
 * call.
 */
public final class CdcToIcebergJob {

    private static final String ICEBERG_GROUP = "iceberg";
    private static final String CDC_GROUP = "cdc";

    // server-id range for the MySQL binlog client. Kept out of the required
    // property set (matches the local SQL's fixed range) but overridable via
    // the cdc group if a reader needs to avoid a collision.
    private static final String DEFAULT_SERVER_ID = "5400-5404";

    public static void main(String[] args) throws Exception {
        final Map<String, Properties> appProps =
                KinesisAnalyticsRuntime.getApplicationProperties();

        final Properties iceberg = requireGroup(appProps, ICEBERG_GROUP);
        final Properties cdc = requireGroup(appProps, CDC_GROUP);

        // --- mode switch -----------------------------------------------------
        // cdc group, key "mode": "single" (default) keeps the single-table
        // Table API path below untouched; "dynamic" hands off to the
        // whole-schema DataStream path in DynamicCdcToIcebergJob, which mirrors
        // every table in the source database into iceberg.<namespace>.<table>,
        // creating the Iceberg tables on the fly. The two paths never mix: a
        // "dynamic" run returns here before any single-table SQL is built.
        final String mode = cdc.getProperty("mode", "single");
        if ("dynamic".equalsIgnoreCase(mode)) {
            DynamicCdcToIcebergJob.run(iceberg, cdc);
            return;
        }
        // "dynamic-legacy": whole-schema sync with LIVE new-table pickup (no
        // savepoint restart) via the legacy Debezium SourceFunction source.
        // See LegacyDynamicCdcToIcebergJob for the why and the semantics.
        if ("dynamic-legacy".equalsIgnoreCase(mode)) {
            LegacyDynamicCdcToIcebergJob.run(iceberg, cdc);
            return;
        }
        // "pg-probe": minimal PostgreSQL new-table live-pickup experiment
        // (print sink, no Iceberg). Additive, off the production paths.
        if ("pg-probe".equalsIgnoreCase(mode)) {
            PgNewTableProbe.run(cdc);
            return;
        }
        if (!"single".equalsIgnoreCase(mode)) {
            throw new IllegalStateException(
                    "Invalid property 'mode' in MSF property group 'cdc': '" + mode
                    + "'. Expected 'single' (default), 'dynamic', or 'dynamic-legacy'.");
        }

        // --- iceberg group ---------------------------------------------------
        final String catalogUri = require(iceberg, ICEBERG_GROUP, "catalog.uri");
        final String warehouse = require(iceberg, ICEBERG_GROUP, "catalog.warehouse");
        final String sigv4Enabled = require(iceberg, ICEBERG_GROUP, "catalog.rest.sigv4-enabled");
        final String signingName = require(iceberg, ICEBERG_GROUP, "catalog.rest.signing-name");
        final String namespace = require(iceberg, ICEBERG_GROUP, "catalog.namespace");
        final String formatVersion = require(iceberg, ICEBERG_GROUP, "catalog.format-version");

        // client.region: the AWS SDK v2 S3FileIO requires an explicit region.
        // Derive it from the S3 Tables REST uri
        // (https://s3tables.<region>.amazonaws.com/iceberg); fall back to the
        // MSF-provided AWS_REGION / AWS_DEFAULT_REGION env for any other uri
        // shape. Fail fast if neither yields a region.
        final String region = resolveRegion(catalogUri);

        // --- cdc group -------------------------------------------------------
        final String hostname = require(cdc, CDC_GROUP, "hostname");
        final String port = require(cdc, CDC_GROUP, "port");
        final String databaseName = require(cdc, CDC_GROUP, "database-name");
        final String tableName = require(cdc, CDC_GROUP, "table-name");
        final String username = require(cdc, CDC_GROUP, "username");
        final String password = require(cdc, CDC_GROUP, "password");
        final String serverId = cdc.getProperty("server-id", DEFAULT_SERVER_ID);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 10s checkpointing — mirrors SET 'execution.checkpointing.interval' in
        // the SQL. NOTE: this must go through TableConfig, not executeSql():
        // Flink's application-mode client rejects SET statements from
        // executeSql() ("Unsupported SQL query!"), which fails main() at
        // submission on Managed Service for Apache Flink. (MSF also enforces
        // its own checkpoint config; this keeps the job self-consistent when
        // run outside MSF.)
        tEnv.getConfig().set("execution.checkpointing.interval", "10s");

        // 1) Iceberg REST catalog pointed at S3 Tables (SigV4). Same shape as
        //    the local SQL's CREATE CATALOG, with the connection props swapped
        //    for the S3 Tables endpoint. No s3.endpoint / path-style / static
        //    keys here — SigV4 + the app role handle auth against real S3.
        // Extra catalog.* props beyond the six explicit ones above are passed
        // through verbatim (prefix stripped). On MSF none are set, so the
        // catalog is unchanged; locally this is how s3.endpoint /
        // s3.path-style-access / s3.access-key-id / s3.secret-access-key reach
        // S3FileIO for the MinIO stand-in (see docker/application_properties_*).
        final StringBuilder extraWith = new StringBuilder();
        for (Map.Entry<String, String> e : extraCatalogProps(iceberg).entrySet()) {
            final String k = e.getKey();
            if (k.equals("uri") || k.equals("warehouse")
                    || k.equals("rest.sigv4-enabled") || k.equals("rest.signing-name")) {
                continue; // already emitted explicitly below
            }
            extraWith.append(String.format(",%n  '%s' = '%s'", k, esc(e.getValue())));
        }

        tEnv.executeSql(String.format(
                "CREATE CATALOG iceberg WITH (%n" +
                "  'type'               = 'iceberg',%n" +
                "  'catalog-type'       = 'rest',%n" +
                "  'uri'                = '%s',%n" +
                "  'warehouse'          = '%s',%n" +
                "  'io-impl'            = 'org.apache.iceberg.aws.s3.S3FileIO',%n" +
                "  'client.region'      = '%s',%n" +
                "  'rest.sigv4-enabled' = '%s',%n" +
                "  'rest.signing-name'  = '%s'%s%n" +
                ")",
                esc(catalogUri), esc(warehouse), esc(region),
                esc(sigv4Enabled), esc(signingName), extraWith.toString()));

        tEnv.executeSql(String.format(
                "CREATE DATABASE IF NOT EXISTS iceberg.%s", ident(namespace)));

        // 2) Iceberg sink table — upsert mode keeps it a faithful mirror of the
        //    source primary key. Identical schema to sql/cdc_to_iceberg.sql.
        tEnv.executeSql(String.format(
                "CREATE TABLE IF NOT EXISTS iceberg.%s.orders (%n" +
                "  order_id   BIGINT,%n" +
                "  customer   STRING,%n" +
                "  sku        STRING,%n" +
                "  qty        INT,%n" +
                "  amount     DECIMAL(10,2),%n" +
                "  status     STRING,%n" +
                "  updated_at TIMESTAMP(3),%n" +
                "  PRIMARY KEY (order_id) NOT ENFORCED%n" +
                ") WITH (%n" +
                "  'format-version'       = '%s',%n" +
                "  'write.upsert.enabled' = 'true'%n" +
                ")",
                ident(namespace), esc(formatVersion)));

        // 3) CDC source — live changelog view of the source table. The WITH
        //    clause is engine-specific: cdc.engine selects the connector
        //    ('mysql' default keeps prior deploys byte-identical; 'postgres'
        //    verified on the same stack). Connection props come from the
        //    "cdc" property group either way.
        final String engine = cdc.getProperty("engine", "mysql");
        final String sourceWith;
        if ("oracle".equals(engine)) {
            // oracle-cdc (LogMiner via Debezium). Config mirrors the locally
            // verified sql/cdc_to_iceberg_oracle.sql exactly:
            //  * url points at the PDB SERVICE (FREEPDB1) so discovery and
            //    snapshot run in the PDB (host:port alone resolves to CDB
            //    root and fails with ORA-65040);
            //  * debezium.database.pdb.name stays set so Debezium moves the
            //    LogMiner session to CDB$ROOT itself;
            //  * online_catalog strategy: no catalog-to-redo dictionary
            //    dumps, the right choice when schema changes are rare;
            //  * identifiers are UPPERCASE (Oracle's unquoted default).
            final String oraDb = databaseName.toUpperCase();
            final String oraSchema =
                cdc.getProperty("schema-name", "CDC").toUpperCase();
            final String oraTable = tableName.toUpperCase();
            sourceWith = String.format(
                "  'connector'     = 'oracle-cdc',%n" +
                "  'hostname'      = '%s',%n" +
                "  'port'          = '%s',%n" +
                "  'username'      = '%s',%n" +
                "  'password'      = '%s',%n" +
                "  'url'           = 'jdbc:oracle:thin:@%s:%s/%s',%n" +
                "  'database-name' = '%s',%n" +
                "  'schema-name'   = '%s',%n" +
                "  'table-name'    = '%s',%n" +
                "  'debezium.database.pdb.name'   = '%s',%n" +
                "  'debezium.log.mining.strategy' = 'online_catalog'%n",
                esc(hostname), esc(port), esc(username), esc(password),
                esc(hostname), esc(port), esc(oraDb),
                esc(oraDb), esc(oraSchema), esc(oraTable), esc(oraDb));
        } else if ("postgres".equals(engine)) {
            // postgres-cdc: schema-qualified table, logical decoding via
            // pgoutput, and a replication slot per job (slot.name is
            // mandatory for the postgres connector).
            sourceWith = String.format(
                "  'connector'                         = 'postgres-cdc',%n" +
                "  'hostname'                          = '%s',%n" +
                "  'port'                              = '%s',%n" +
                "  'username'                          = '%s',%n" +
                "  'password'                          = '%s',%n" +
                "  'database-name'                     = '%s',%n" +
                "  'schema-name'                       = '%s',%n" +
                "  'table-name'                        = '%s',%n" +
                "  'slot.name'                         = '%s',%n" +
                "  'decoding.plugin.name'              = 'pgoutput',%n" +
                // Upsert changelog mode: PG tables default to REPLICA IDENTITY
                // DEFAULT (PK-only before-images). The connector's default
                // 'all' mode requires REPLICA IDENTITY FULL and crash-loops on
                // the first UPDATE/DELETE otherwise ("before" field is null --
                // hit empirically on MSF). 'upsert' emits changes keyed by PK
                // with no UPDATE_BEFORE, which the Iceberg upsert sink applies
                // correctly, and works against unmodified source tables.
                "  'changelog-mode'                    = 'upsert',%n" +
                "  'scan.incremental.snapshot.enabled' = 'true'%n",
                esc(hostname), esc(port), esc(username), esc(password),
                esc(databaseName),
                esc(cdc.getProperty("schema-name", "public")),
                esc(tableName),
                esc(cdc.getProperty("slot.name", "flink_cdc_orders")));
        } else if ("mysql".equals(engine)) {
            sourceWith = String.format(
                "  'connector'                         = 'mysql-cdc',%n" +
                "  'hostname'                          = '%s',%n" +
                "  'port'                              = '%s',%n" +
                "  'username'                          = '%s',%n" +
                "  'password'                          = '%s',%n" +
                "  'database-name'                     = '%s',%n" +
                "  'table-name'                        = '%s',%n" +
                "  'server-id'                         = '%s',%n" +
                "  'scan.incremental.snapshot.enabled' = 'true'%n",
                esc(hostname), esc(port), esc(username), esc(password),
                esc(databaseName), esc(tableName), esc(serverId));
        } else {
            throw new IllegalArgumentException(
                "cdc.engine must be 'mysql', 'postgres', or 'oracle' for "
                + "mode=single (got '" + engine + "').");
        }
        // Oracle emits UPPERCASE field names (unquoted-identifier default),
        // and Flink maps Debezium fields to columns BY NAME -- so the source
        // table's column case must match the engine. The INSERT below maps
        // source to sink by position, so the sink stays lowercase.
        final String srcCols = "oracle".equals(engine)
            ? "  ORDER_ID   BIGINT,%n" +
              "  CUSTOMER   STRING,%n" +
              "  SKU        STRING,%n" +
              "  QTY        INT,%n" +
              "  AMOUNT     DECIMAL(10,2),%n" +
              "  STATUS     STRING,%n" +
              "  UPDATED_AT TIMESTAMP(3),%n" +
              "  PRIMARY KEY (ORDER_ID) NOT ENFORCED%n"
            : "  order_id   BIGINT,%n" +
              "  customer   STRING,%n" +
              "  sku        STRING,%n" +
              "  qty        INT,%n" +
              "  amount     DECIMAL(10,2),%n" +
              "  status     STRING,%n" +
              "  updated_at TIMESTAMP(3),%n" +
              "  PRIMARY KEY (order_id) NOT ENFORCED%n";
        tEnv.executeSql(String.format(
                "CREATE TEMPORARY TABLE orders_src (%n" + srcCols
                + ") WITH (%n%s)", sourceWith));

        // 4) Start the continuous sync through a statement set — the canonical
        //    Table API pattern on Managed Service for Apache Flink: the
        //    application must produce a single job graph, and a StatementSet
        //    is how multiple INSERTs share it (see the post's "Syncing more
        //    than one table" section — add one addInsertSql per table).
        final StatementSet statements = tEnv.createStatementSet();
        statements.addInsertSql(String.format(
                "INSERT INTO iceberg.%s.orders SELECT * FROM orders_src",
                ident(namespace)));
        statements.execute();
    }

    /** Look up a required property group or fail fast naming it. */
    static Properties requireGroup(Map<String, Properties> all, String group) {
        final Properties p = all.get(group);
        if (p == null) {
            throw new IllegalStateException(
                    "Missing MSF application property group '" + group + "'. "
                    + "Configure it under the application's environment properties "
                    + "(see cdk/lib/stack.ts).");
        }
        return p;
    }

    /** Look up a required key within a group or fail fast naming group + key. */
    static String require(Properties p, String group, String key) {        final String v = p.getProperty(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException(
                    "Missing required property '" + key + "' in MSF property group '"
                    + group + "'. Set it in the application's environment properties "
                    + "(see cdk/lib/stack.ts).");
        }
        return v;
    }

    /**
     * Every {@code catalog.*} key in the iceberg property group is passed
     * through to the Iceberg catalog with the {@code catalog.} prefix stripped
     * (e.g. {@code catalog.s3.endpoint} -> {@code s3.endpoint}), EXCEPT the
     * two job-level keys that are not catalog properties
     * ({@code catalog.namespace}, {@code catalog.format-version}). On Managed
     * Service for Apache Flink only the six documented keys are set, so the
     * catalog is unchanged; locally this lets the MinIO S3 stand-in props
     * (s3.endpoint / s3.path-style-access / s3.access-key-id /
     * s3.secret-access-key) reach S3FileIO without any bespoke handling.
     */
    static Map<String, String> extraCatalogProps(Properties iceberg) {
        final Set<String> skip = Set.of("catalog.namespace", "catalog.format-version");
        final Map<String, String> extra = new LinkedHashMap<>();
        for (String name : iceberg.stringPropertyNames()) {
            if (name.startsWith("catalog.") && !skip.contains(name)) {
                final String v = iceberg.getProperty(name);
                if (v != null && !v.isEmpty()) {
                    extra.put(name.substring("catalog.".length()), v);
                }
            }
        }
        return extra;
    }

    /**
     * Derive the AWS region from the S3 Tables REST uri
     * ({@code https://s3tables.<region>.amazonaws.com/iceberg}). Falls back to
     * the MSF-provided AWS_REGION / AWS_DEFAULT_REGION env vars. Fails fast if
     * no region can be resolved — S3FileIO cannot start without one.
     */
    static String resolveRegion(String catalogUri) {
        final Matcher m = Pattern
                .compile("^https?://s3tables\\.([a-z0-9-]+)\\.amazonaws\\.com")
                .matcher(catalogUri);
        if (m.find()) {
            return m.group(1);
        }
        final String env = firstNonEmpty(
                System.getenv("AWS_REGION"), System.getenv("AWS_DEFAULT_REGION"));
        if (env != null) {
            return env;
        }
        throw new IllegalStateException(
                "Could not resolve an AWS region: catalog.uri '" + catalogUri
                + "' is not an S3 Tables endpoint and neither AWS_REGION nor "
                + "AWS_DEFAULT_REGION is set. S3FileIO requires an explicit region "
                + "(see README: 'S3FileIO needs an explicit region').");
    }

    static String firstNonEmpty(String a, String b) {
        if (a != null && !a.isEmpty()) return a;
        if (b != null && !b.isEmpty()) return b;
        return null;
    }

    /** Escape a single-quoted SQL literal value. */
    private static String esc(String v) {
        return v.replace("'", "''");
    }

    /** Guard a SQL identifier (namespace) against injection via props. */
    static String ident(String v) {
        if (!v.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalStateException(
                    "Invalid catalog.namespace '" + v + "': must be a simple SQL "
                    + "identifier (letters, digits, underscore).");
        }
        return v;
    }

    private CdcToIcebergJob() {}
}
