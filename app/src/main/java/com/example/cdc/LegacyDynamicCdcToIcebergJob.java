package com.example.cdc;

import org.apache.flink.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
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
 * Experimental whole-schema variant that gives LIVE new-table pickup (no
 * savepoint restart), reached when the MSF app sets {@code cdc.mode =
 * dynamic-legacy}.
 *
 * <p>Why a second dynamic path exists. The default {@code dynamic} path
 * ({@link DynamicCdcToIcebergJob}) uses the incremental-snapshot DataStream
 * source ({@code org.apache.flink.cdc.connectors.mysql.source.MySqlSource}). In
 * Flink&nbsp;CDC&nbsp;3.6.0-2.2 that source freezes its captured-table set at
 * job start: its {@code BinlogSplitReader.shouldEmit()} filters binlog events
 * against a fixed table list, and {@code scan.newly-added-table.enabled} only
 * re-reads the catalog on a savepoint/checkpoint restore. The binlog-live
 * option {@code scan.binlog.newly-added-table.enabled} (upstream FLINK-36115,
 * pipeline connector) is NOT present in the 2.x DataStream artifact, and no
 * newer {@code -2.x} connector line is published on Maven Central. So there is
 * no incremental-source route to live pickup on Flink 2.x.
 *
 * <p>This path instead uses the LEGACY Debezium {@code SourceFunction} source
 * ({@code org.apache.flink.cdc.connectors.mysql.MySqlSource}, builder
 * {@code MySqlSource.builder()}). It runs Debezium's embedded engine directly
 * and applies the {@code table.include.list} regex ({@code <db>.*}) inside
 * Debezium at binlog-read time -- there is no frozen Flink-side split list. A
 * table created after the job starts, whose name matches the regex, is
 * therefore captured live from the binlog exactly as standalone Debezium would,
 * with no Flink restart.
 *
 * <p>Everything downstream of the source is IDENTICAL to {@link
 * DynamicCdcToIcebergJob}: the same {@link CdcJsonDeserializer} (both sources
 * accept a {@link org.apache.flink.cdc.debezium.DebeziumDeserializationSchema}),
 * the same {@link CdcDynamicRecordGenerator}, and the same {@link
 * DynamicIcebergSink} on the same Iceberg REST catalog. Only the source object
 * differs.
 *
 * <p>Semantics / caveats (documented for the blog):
 * <ul>
 *   <li><b>Existing tables</b> at start get a full snapshot then binlog
 *       ({@code snapshot.mode=initial}, overridable via
 *       {@code cdc.debezium.snapshot.mode}).</li>
 *   <li><b>A table created after start</b> is captured binlog-only: its rows
 *       flow as they are written; there is NO retroactive snapshot of rows that
 *       existed before Debezium first saw the table. (A table created and
 *       populated entirely after start is fully captured, since every row is a
 *       binlog INSERT.)</li>
 *   <li>The legacy {@code SourceFunction} is non-parallel (parallelism 1) and is
 *       deprecated upstream in favour of the incremental source; it is offered
 *       here as the ONLY way to get live pickup on this connector line, not as
 *       the recommended production default.</li>
 * </ul>
 */
public final class LegacyDynamicCdcToIcebergJob {

    private static final String ICEBERG_GROUP = "iceberg";
    private static final String CDC_GROUP = "cdc";
    private static final int DEFAULT_SERVER_ID = 5500;

    private LegacyDynamicCdcToIcebergJob() {}

    /**
     * Build and run the live whole-schema pipeline. Called from
     * {@link CdcToIcebergJob#main(String[])} when {@code cdc.mode =
     * dynamic-legacy}; the two property groups are already resolved there.
     */
    static void run(Properties iceberg, Properties cdc) throws Exception {

        // --- iceberg group (identical keys to the other paths) ---------------
        final String catalogUri = require(iceberg, ICEBERG_GROUP, "catalog.uri");
        final String namespace = ident(require(iceberg, ICEBERG_GROUP, "catalog.namespace"));
        final String formatVersion = require(iceberg, ICEBERG_GROUP, "catalog.format-version");
        final String region = resolveRegion(catalogUri);

        // --- cdc group -------------------------------------------------------
        final String hostname = require(cdc, CDC_GROUP, "hostname");
        final String port = require(cdc, CDC_GROUP, "port");
        final String databaseName = require(cdc, CDC_GROUP, "database-name");
        final String username = require(cdc, CDC_GROUP, "username");
        final String password = require(cdc, CDC_GROUP, "password");
        final int serverId = parseServerId(cdc.getProperty("server-id"));

        // Debezium engine props. snapshot.mode=initial => snapshot existing
        // tables then stream; a table appearing later is captured binlog-only.
        final Properties dbz = new Properties();
        dbz.setProperty("snapshot.mode",
                cdc.getProperty("debezium.snapshot.mode", "initial"));
        // MySQL 8.x defaults to caching_sha2_password; over a non-TLS JDBC
        // connection the driver refuses to fetch the server's public key unless
        // told to. The incremental source sets this internally; the legacy
        // SourceFunction needs it passed through explicitly. ssl.mode=disabled
        // matches the plaintext local/compose network.
        dbz.setProperty("database.allowPublicKeyRetrieval", "true");
        dbz.setProperty("database.ssl.mode", "disabled");
        // Readable decimals: Debezium's default "precise" mode emits DECIMAL /
        // NUMERIC as base64 unscaled bytes, which the JSON inference lands as
        // opaque base64 strings in Iceberg. "double" emits plain JSON numbers.
        // Same default + override key as the incremental dynamic path.
        dbz.setProperty("decimal.handling.mode",
                cdc.getProperty("debezium.decimal.handling.mode", "double"));

        // Legacy Debezium SourceFunction source. tableList is the Debezium
        // table.include.list regex; "<db>.*" captures every current AND future
        // table in the database live, because Debezium re-applies the regex to
        // binlog events as they arrive (no frozen Flink split list).
        final DebeziumSourceFunction<String> source = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(Integer.parseInt(port))
                .databaseList(databaseName)
                .tableList(databaseName + ".*")
                .username(username)
                .password(password)
                .serverId(serverId)
                // Reuse the exact deserializer the incremental path uses: it
                // wraps the Debezium envelope + PK column names as JSON.
                .deserializer(new CdcJsonDeserializer())
                .debeziumProperties(dbz)
                .build();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        // env.addSource(SourceFunction): the legacy source is a
        // RichSourceFunction, so it attaches via addSource (not fromSource).
        final DataStream<String> changes = env
                .addSource(source, "MySQL CDC legacy (whole schema live: "
                        + databaseName + ".*)")
                .uid("mysql-cdc-source-dynamic-legacy");

        final CatalogLoader catalogLoader = restCatalogLoader(iceberg, region);

        // IDENTICAL sink wiring to DynamicCdcToIcebergJob.
        DynamicIcebergSink.forInput(changes)
                .generator(new CdcDynamicRecordGenerator(namespace, formatVersion))
                .catalogLoader(catalogLoader)
                .immediateTableUpdate(true)
                .set("write.format.default", "parquet")
                .set("format-version", formatVersion)
                .uidPrefix("dynamic-iceberg-sink")
                .append();

        env.execute("Dynamic CDC (legacy live) -> Iceberg on S3 Tables (whole schema)");
    }

    /**
     * The legacy builder's {@code serverId(int)} takes a single value (unlike
     * the incremental source's range string). Accept either form from the
     * {@code server-id} property: a bare int, or the low end of a "a-b" range.
     */
    private static int parseServerId(String raw) {
        if (raw == null || raw.isEmpty()) {
            return DEFAULT_SERVER_ID;
        }
        final int dash = raw.indexOf('-');
        final String first = dash >= 0 ? raw.substring(0, dash) : raw;
        try {
            return Integer.parseInt(first.trim());
        } catch (NumberFormatException e) {
            return DEFAULT_SERVER_ID;
        }
    }

    /**
     * Same Iceberg REST catalog + property pass-through as {@link
     * DynamicCdcToIcebergJob}; duplicated here (a few lines) so the existing
     * {@code dynamic} path's class stays byte-for-byte unchanged.
     */
    private static CatalogLoader restCatalogLoader(Properties iceberg, String region) {
        final Map<String, String> props =
                new HashMap<>(CdcToIcebergJob.extraCatalogProps(iceberg));
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("client.region", region);
        return CatalogLoader.rest("iceberg", new Configuration(false), props);
    }
}
