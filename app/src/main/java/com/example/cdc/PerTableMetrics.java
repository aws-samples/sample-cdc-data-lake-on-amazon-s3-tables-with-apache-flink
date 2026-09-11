package com.example.cdc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pass-through stage that registers <b>per-table custom metrics</b> for the
 * whole-schema sync. The dynamic sink routes every table through one operator,
 * so no Flink monitoring level can tell you which source table is flowing;
 * these counters can.
 *
 * <p>Registered via the standard Flink metric system, so on Amazon Managed
 * Service for Apache Flink they are published to CloudWatch automatically as
 * custom metrics (metric name {@code recordsProcessed}, one per
 * {@code cdcTable} group). Keep source-table cardinality in mind: one
 * CloudWatch custom metric per table per parallel subtask.
 *
 * <p>The table name is read from the Debezium envelope's
 * {@code source.table} field with a cheap regex rather than a full JSON parse:
 * this stage sits on the hot path and the generator downstream re-parses the
 * envelope anyway.
 */
final class PerTableMetrics extends RichMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    // "source":{..."table":"orders"...} -- table is the first "table" key
    // inside the source block. Envelope shape is stable Debezium JSON.
    private static final Pattern TABLE = Pattern.compile(
            "\"source\"\\s*:\\s*\\{[^}]*?\"table\"\\s*:\\s*\"([^\"]+)\"");
    // source.ts_ms = commit time at the database (event time). The top-level
    // ts_ms is connector processing time -- not what lag should measure.
    private static final Pattern SOURCE_TS = Pattern.compile(
            "\"source\"\\s*:\\s*\\{[^}]*?\"ts_ms\"\\s*:\\s*(\\d+)");
    private static final Pattern OP = Pattern.compile("\"op\"\\s*:\\s*\"([cudr])\"");

    private transient Map<String, Counter> counters;
    private transient Map<String, Counter> opCounters;
    private volatile long eventTimeLagMs;

    @Override
    public String map(String envelope) {
        if (counters == null) {
            // Lazy init instead of open(): Flink 2.x replaced the
            // open(Configuration) signature with open(OpenContext).
            counters = new HashMap<>();
            opCounters = new HashMap<>();
            // Event-time lag of the last record seen: now - source commit
            // time. THE production "is the job keeping up" signal, since the
            // connector's own currentFetchEventTimeLag never reaches
            // CloudWatch (verified: Flink-REST-only on MSF).
            getRuntimeContext().getMetricGroup()
                    .addGroup("kinesisanalytics")
                    .gauge("eventTimeLagMs", () -> eventTimeLagMs);
        }
        final Matcher ts = SOURCE_TS.matcher(envelope);
        if (ts.find()) {
            final long srcTs = Long.parseLong(ts.group(1));
            // Snapshot-phase records can carry source.ts_ms = 0; skipping them
            // keeps the gauge from spiking to epoch-now (observed under load).
            if (srcTs > 0) {
                eventTimeLagMs = Math.max(0L, System.currentTimeMillis() - srcTs);
            }
        }
        // Job-wide operation mix (c/u/d, r = snapshot read): deliberately NOT
        // per-table to keep CloudWatch cardinality flat as table count grows.
        final Matcher op = OP.matcher(envelope);
        if (op.find()) {
            final String name;
            switch (op.group(1)) {
                case "c": name = "insertsProcessed"; break;
                case "u": name = "updatesProcessed"; break;
                case "d": name = "deletesProcessed"; break;
                default:  name = "snapshotRowsProcessed"; break;
            }
            opCounters.computeIfAbsent(name, n ->
                    getRuntimeContext().getMetricGroup()
                            .addGroup("kinesisanalytics")
                            .counter(n))
                    .inc();
        }
        final Matcher m = TABLE.matcher(envelope);
        if (m.find()) {
            counters.computeIfAbsent(m.group(1), t ->
                    getRuntimeContext().getMetricGroup()
                            // MSF publishes user metrics to CloudWatch ONLY
                            // when they are registered under a group named
                            // "kinesisanalytics" (verified empirically: without
                            // it the metric exists in the Flink REST API but
                            // never reaches CloudWatch). Nested key/value
                            // groups become CloudWatch dimensions.
                            .addGroup("kinesisanalytics")
                            .addGroup("cdcTable", t)
                            .counter("recordsProcessed"))
                    .inc();
        }
        return envelope;
    }
}
