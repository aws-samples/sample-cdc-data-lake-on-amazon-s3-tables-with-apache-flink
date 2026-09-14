package com.example.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.example.cdc.CdcToIcebergJob.require;

/**
 * Whole-schema PostgreSQL CDC into Apache Iceberg on Amazon S3 Tables:
 * mode = "dynamic" with engine = "postgres". The pipeline downstream of the
 * source is IDENTICAL to the MySQL dynamic path -- {@link CdcJsonDeserializer}
 * operates on the engine-neutral Kafka Connect SourceRecord, so schema
 * inference, semantic type handling, per-record upsert routing, and Iceberg
 * table auto-creation are shared code ({@link CdcDynamicRecordGenerator},
 * {@link DynamicIcebergSink}).
 *
 * <p>New-table pickup is LIVE, like MySQL: the connector's Debezium creates a
 * {@code FOR ALL TABLES} publication by default, and the schema-regex
 * tableList matches tables created after the job starts. A post-start table
 * needs no snapshot because its entire history is in the WAL (verified: a
 * table created against the running job lands in Iceberg with its rows,
 * same jobId, no restart; rows written while the job is down arrive on
 * checkpoint restore).
 *
 * <p>Replication-slot retention: the slot advances only when a captured-table
 * record is emitted AND a checkpoint completes, so a schema that goes idle
 * while the server writes WAL elsewhere pins WAL without bound (Debezium's
 * heartbeat options do not close this gap through Flink CDC -- see the README
 * section "PostgreSQL replication-slot retention"). Because dynamic mode
 * captures the whole schema, the remedy is one heartbeat table INSIDE the
 * schema updated on a schedule from the database side. Set
 * {@code cdc.heartbeat-table} to its name to keep those bookkeeping rows out
 * of the lake: heartbeat records still advance the slot (emission happens at
 * the source) but are dropped before the sink.
 *
 * <p>Deletes: with the default {@code REPLICA IDENTITY}, delete events carry
 * the primary key, which is exactly what the per-record equality delete
 * needs; no {@code ALTER TABLE ... REPLICA IDENTITY FULL} is required.
 * The fresh-submission caveat from the README (deletes during downtime
 * survive as orphans after a re-snapshot; checkpoint/snapshot restore
 * replays them correctly) applies unchanged.
 */
public final class PgDynamicCdcToIcebergJob {

    private static final String ICEBERG_GROUP = "iceberg";
    private static final String CDC_GROUP = "cdc";

    private PgDynamicCdcToIcebergJob() {}

    static void run(Properties iceberg, Properties cdc) throws Exception {
        final String catalogUri = require(iceberg, ICEBERG_GROUP, "catalog.uri");
        final String namespace = require(iceberg, ICEBERG_GROUP, "catalog.namespace");
        final String formatVersion = require(iceberg, ICEBERG_GROUP, "catalog.format-version");
        final String region = CdcToIcebergJob.resolveRegion(catalogUri);

        final String hostname = require(cdc, CDC_GROUP, "hostname");
        final String port = require(cdc, CDC_GROUP, "port");
        final String database = require(cdc, CDC_GROUP, "database-name");
        final String username = require(cdc, CDC_GROUP, "username");
        final String password = require(cdc, CDC_GROUP, "password");
        final String schema = cdc.getProperty("schema-name", "public");
        final String slotName = cdc.getProperty("slot-name", "flink_cdc_dynamic");
        final String plugin = cdc.getProperty("decoding.plugin.name", "pgoutput");
        final String heartbeatTable = cdc.getProperty("heartbeat-table", "");

        // Same awkward-encoding default as the MySQL dynamic path: DECIMAL/
        // NUMERIC as readable doubles unless overridden (see the README's
        // dynamic-mode configuration decisions).
        final Properties dbz = new Properties();
        dbz.setProperty("decimal.handling.mode",
                cdc.getProperty("debezium.decimal.handling.mode", "double"));

        final PostgresSourceBuilder.PostgresIncrementalSource<String> source =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(hostname)
                        .port(Integer.parseInt(port))
                        .database(database)
                        .schemaList(schema)
                        // Schema regex: every table in the schema, including
                        // tables created after the job starts.
                        .tableList(schema + ".*")
                        .username(username)
                        .password(password)
                        .slotName(slotName)
                        .decodingPluginName(plugin)
                        .debeziumProperties(dbz)
                        .deserializer(new CdcJsonDeserializer())
                        .build();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        DataStream<String> changes = env
                .fromSource(source, WatermarkStrategy.noWatermarks(),
                        "PostgreSQL CDC (whole schema: " + schema + ".*)")
                .uid("pg-cdc-source-dynamic")
                .map(new PerTableMetrics())
                .name("per-table-metrics")
                .uid("per-table-metrics");

        if (!heartbeatTable.isEmpty()) {
            changes = changes
                    .filter(new HeartbeatTableFilter(heartbeatTable))
                    .name("drop-heartbeat-table")
                    .uid("drop-heartbeat-table");
        }

        final CatalogLoader catalogLoader =
                DynamicCdcToIcebergJob.restCatalogLoader(iceberg, region);

        DynamicIcebergSink.forInput(changes)
                .generator(new CdcDynamicRecordGenerator(namespace, formatVersion))
                .catalogLoader(catalogLoader)
                .immediateTableUpdate(true)
                .set("write.format.default", "parquet")
                .set("format-version", formatVersion)
                .uidPrefix("dynamic-iceberg-sink")
                .append();

        env.execute("Dynamic CDC -> Iceberg on S3 Tables (PostgreSQL, whole schema)");
    }

    /**
     * Drops records from the heartbeat table before the sink. The heartbeat
     * table exists to advance the replication slot (which happens at the
     * source, before this filter), not to be mirrored into the lake. Same
     * cheap source.table regex as {@link PerTableMetrics}: this sits on the
     * hot path and the generator re-parses the envelope anyway.
     */
    static final class HeartbeatTableFilter implements FilterFunction<String> {
        private static final long serialVersionUID = 1L;
        private static final Pattern TABLE = Pattern.compile(
                "\"source\"\\s*:\\s*\\{[^}]*?\"table\"\\s*:\\s*\"([^\"]+)\"");
        private final String heartbeatTable;

        HeartbeatTableFilter(String heartbeatTable) {
            this.heartbeatTable = heartbeatTable;
        }

        @Override
        public boolean filter(String json) {
            final Matcher m = TABLE.matcher(json);
            return !(m.find() && heartbeatTable.equals(m.group(1)));
        }
    }
}
