package com.example.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.example.cdc.CdcToIcebergJob.require;

/**
 * MINIMAL PostgreSQL new-table live-pickup probe (NOT wired into the
 * Iceberg mode dispatch's business logic -- reached only when cdc.mode =
 * "pg-probe"). It builds the incremental-snapshot {@link PostgresSourceBuilder}
 * over the whole "public" schema and PRINTS the Debezium source.table of every
 * change record. No Iceberg, no sink beyond print -- the sole question is
 * whether a table CREATEd after the job starts shows up live in the stream when
 * scanNewlyAddedTableEnabled is OFF (the mirror of the MySQL experiment).
 */
public final class PgNewTableProbe {

    private static final String CDC_GROUP = "cdc";
    private static final Pattern TABLE = Pattern.compile("\"table\"\\s*:\\s*\"([^\"]+)\"");

    private PgNewTableProbe() {}

    static void run(Properties cdc) throws Exception {
        final String hostname = require(cdc, CDC_GROUP, "hostname");
        final String port = require(cdc, CDC_GROUP, "port");
        final String database = require(cdc, CDC_GROUP, "database-name");
        final String username = require(cdc, CDC_GROUP, "username");
        final String password = require(cdc, CDC_GROUP, "password");
        final String schema = cdc.getProperty("schema-name", "public");
        final String slotName = cdc.getProperty("slot-name", "flink_pg_probe");
        final String plugin = cdc.getProperty("decoding.plugin.name", "pgoutput");
        final boolean scanNewlyAdded = Boolean.parseBoolean(
                cdc.getProperty("scan.newly-added-table.enabled", "false"));

        final PostgresSourceBuilder.PostgresIncrementalSource<String> source =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(hostname)
                        .port(Integer.parseInt(port))
                        .database(database)
                        .schemaList(schema)
                        .tableList(schema + ".*")
                        .username(username)
                        .password(password)
                        .slotName(slotName)
                        .decodingPluginName(plugin)
                        .scanNewlyAddedTableEnabled(scanNewlyAdded)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // Incremental snapshot source needs checkpointing to finish snapshot
        // and transition to the streaming (WAL) phase.
        env.enableCheckpointing(5_000L);
        env.setParallelism(1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(),
                        "PG CDC probe (whole schema: " + schema + ".*)")
                .map((MapFunction<String, String>) json -> {
                    final Matcher m = TABLE.matcher(json);
                    final String tbl = m.find() ? m.group(1) : "?";
                    return "PGPROBE_REC table=" + schema + "." + tbl;
                })
                .print();

        env.execute("PG new-table live-pickup probe (scanNewlyAdded=" + scanNewlyAdded + ")");
    }
}
