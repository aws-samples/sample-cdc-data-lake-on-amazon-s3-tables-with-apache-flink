package com.example.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.example.cdc.CdcToIcebergJob.ident;
import static com.example.cdc.CdcToIcebergJob.require;
import static com.example.cdc.CdcToIcebergJob.resolveRegion;

/**
 * Whole-schema (dynamic) variant of the blog's CDC path, reached when the MSF
 * app sets {@code cdc.mode = dynamic}. It mirrors the reference sample
 * {@code aws-samples/sample-streaming-data-lake-with-apache-iceberg-and-apache-flink}
 * (dynamic-sink-sample): a DataStream source feeds Iceberg's
 * {@link DynamicIcebergSink}, which routes each record to a table chosen at
 * runtime and creates/evolves that table on the fly.
 *
 * <p>Differences from the reference sample, all inherent to CDC (the reference
 * reads plain JSON from Kinesis):
 * <ul>
 *   <li>Source is Flink CDC {@link MySqlSource} over the whole database
 *       ({@code .databaseList(db)} + {@code .tableList(db + ".*")}), not Kinesis.</li>
 *   <li>Routing key is the Debezium {@code source.table} (one Iceberg table per
 *       source table: {@code iceberg.<namespace>.<source_table>}), not an
 *       {@code event_type} field.</li>
 *   <li>Records carry a {@code RowKind} and a primary key, so the sink runs in
 *       per-record upsert mode (see {@link CdcDynamicRecordGenerator}).</li>
 * </ul>
 * The Iceberg schema-inference and JSON&rarr;RowData conversion are copied from
 * the reference's {@code SchemaAgnosticRoutingGenerator} (see
 * {@link CdcDynamicRecordGenerator}); only the CDC envelope handling is new.
 *
 * <p>The destination namespace ({@code iceberg.<namespace>}) is created by the
 * CDK stack ({@code AWS::S3Tables::Namespace}); the sink creates each table
 * inside it on first sight. The catalog is the SAME Iceberg REST catalog on
 * Amazon S3 Tables (SigV4) as the single-table path -- identical properties.
 */
public final class DynamicCdcToIcebergJob {

    private static final String ICEBERG_GROUP = "iceberg";
    private static final String CDC_GROUP = "cdc";
    private static final String DEFAULT_SERVER_ID = "5400-5404";

    private DynamicCdcToIcebergJob() {}

    /**
     * Build and run the whole-schema pipeline. Called from
     * {@link CdcToIcebergJob#main(String[])} when {@code cdc.mode = dynamic};
     * the two property groups are already resolved there.
     */
    static void run(Properties iceberg, Properties cdc) throws Exception {

        // --- iceberg group (identical keys to the single-table path) ---------
        final String catalogUri = require(iceberg, ICEBERG_GROUP, "catalog.uri");
        final String warehouse = require(iceberg, ICEBERG_GROUP, "catalog.warehouse");
        final String sigv4Enabled = require(iceberg, ICEBERG_GROUP, "catalog.rest.sigv4-enabled");
        final String signingName = require(iceberg, ICEBERG_GROUP, "catalog.rest.signing-name");
        final String namespace = ident(require(iceberg, ICEBERG_GROUP, "catalog.namespace"));
        final String formatVersion = require(iceberg, ICEBERG_GROUP, "catalog.format-version");
        final String region = resolveRegion(catalogUri);

        // --- cdc group -------------------------------------------------------
        // Note: "table-name" is intentionally NOT read here -- the dynamic path
        // captures the whole database, so a single table name is meaningless.
        final String hostname = require(cdc, CDC_GROUP, "hostname");
        final String port = require(cdc, CDC_GROUP, "port");
        final String databaseName = require(cdc, CDC_GROUP, "database-name");
        final String username = require(cdc, CDC_GROUP, "username");
        final String password = require(cdc, CDC_GROUP, "password");
        final String serverId = cdc.getProperty("server-id", DEFAULT_SERVER_ID);

        // Newly-added-table pickup.
        //
        // Flink CDC 3.6.0-2.2 exposes exactly ONE newly-added-table knob:
        // MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED
        // ("scan.newly-added-table.enabled"), surfaced on the builder as
        // scanNewlyAddedTableEnabled(boolean). Its own Javadoc: "Whether to
        // capture the newly added tables or not ... only useful when we start
        // the job from a savepoint/checkpoint."
        //
        // Runtime semantics with this enabled + a "<db>.*" tableList:
        //   * Tables that already exist and match the regex are captured from
        //     job start (default startup = initial => snapshot + binlog).
        //   * A table created AFTER the job started is picked up on the NEXT
        //     stop-with-savepoint + restart, NOT live: on restart the source
        //     re-reads the catalog, finds the new match, SNAPSHOTS its existing
        //     rows, then continues from the binlog. So new tables get a full
        //     snapshot (not binlog-only), but a savepoint restart is REQUIRED
        //     to trigger the pickup.
        //
        // The alternative no-restart, binlog-only option
        // (scan.binlog.newly-added-table.enabled) does NOT exist in the
        // 3.6.0-2.2 artifact set (verified: absent from MySqlSourceOptions and
        // the connector jar), so it cannot be enabled here.
        final boolean scanNewlyAddedTable = Boolean.parseBoolean(
                cdc.getProperty("scan.newly-added-table.enabled", "true"));

        // Debezium engine props. decimal.handling.mode governs how DECIMAL /
        // NUMERIC columns appear in the change envelope. Debezium's default
        // ("precise") emits base64-encoded unscaled bytes -- the JSON schema
        // inference in CdcDynamicRecordGenerator then lands them in Iceberg as
        // opaque base64 strings (verified on MSF: 'AfQ=' instead of 5.00).
        // "double" emits plain JSON numbers, which infer as readable DOUBLE
        // columns. Override via cdc.debezium.decimal.handling.mode if the
        // precision loss of double matters (e.g. "string" for lossless text).
        //
        // NOTE: changing this against tables ALREADY created under "precise"
        // is a schema clash (string -> double is an illegal column type
        // change). Point the job at a fresh namespace when switching modes.
        final Properties dbz = new Properties();
        dbz.setProperty("decimal.handling.mode",
                cdc.getProperty("debezium.decimal.handling.mode", "double"));

        final MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(Integer.parseInt(port))
                .databaseList(databaseName)
                // Whole schema: every table in the database. The mysql-cdc
                // tableList is "<db>.<table>" regex, so "<db>.*" = all tables.
                .tableList(databaseName + ".*")
                .username(username)
                .password(password)
                .serverId(serverId)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTable)
                .debeziumProperties(dbz)
                // Emit the Debezium change envelope as JSON and stamp each row
                // with its primary-key column names (from the Kafka Connect key
                // schema) so the sink can upsert per table. See CdcJsonDeserializer.
                .deserializer(new CdcJsonDeserializer())
                .build();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // Mirrors the single-table path's 10s checkpointing so the job is
        // self-consistent when run outside MSF. On Managed Service for Apache
        // Flink the service-managed checkpoint config takes precedence.
        env.enableCheckpointing(10_000L);

        final DataStream<String> changes = env
                .fromSource(source, WatermarkStrategy.noWatermarks(),
                        "MySQL CDC (whole schema: " + databaseName + ".*)")
                .uid("mysql-cdc-source-dynamic");

        final CatalogLoader catalogLoader = restCatalogLoader(iceberg, region);

        // DynamicIcebergSink wiring mirrors the reference dynamic-sink-sample:
        // forInput -> generator -> catalogLoader -> immediateTableUpdate -> set(...)
        // -> uidPrefix -> append(). Upsert vs append and the equality (primary
        // key) fields are decided PER RECORD inside the generator via
        // DynamicRecord.setUpsertMode/ setEqualityFields, so append-only tables
        // (no primary key) and CDC tables coexist without a global upsert flag.
        DynamicIcebergSink.forInput(changes)
                .generator(new CdcDynamicRecordGenerator(namespace, formatVersion))
                .catalogLoader(catalogLoader)
                .immediateTableUpdate(true)
                .set("write.format.default", "parquet")
                .set("format-version", formatVersion)
                .uidPrefix("dynamic-iceberg-sink")
                .append();

        env.execute("Dynamic CDC -> Iceberg on S3 Tables (whole schema)");
    }

    /**
     * Iceberg REST catalog on Amazon S3 Tables (SigV4) -- the SAME connection
     * properties as the single-table path's {@code CREATE CATALOG}, expressed
     * as a {@link CatalogLoader} for the DataStream sink. Every {@code catalog.*}
     * key from the iceberg property group is passed through (prefix stripped)
     * via {@link CdcToIcebergJob#extraCatalogProps(Properties)}: on MSF that is
     * uri / warehouse / rest.sigv4-enabled / rest.signing-name; locally it also
     * carries the MinIO S3 stand-in props (s3.endpoint / s3.path-style-access /
     * s3.access-key-id / s3.secret-access-key) so S3FileIO talks to MinIO
     * instead of real S3. io-impl and client.region are always set.
     */
    private static CatalogLoader restCatalogLoader(Properties iceberg, String region) {
        final Map<String, String> props =
                new HashMap<>(CdcToIcebergJob.extraCatalogProps(iceberg));
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("client.region", region);
        // new Configuration(false): matches the vendored HadoopUtils behaviour
        // that makes the catalog load on MSF. The class is relocated to
        // shaded.org.apache.hadoop.conf.Configuration by the shade plugin.
        return CatalogLoader.rest("iceberg", new Configuration(false), props);
    }
}
