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

    private transient Map<String, Counter> counters;

    @Override
    public String map(String envelope) {
        if (counters == null) {
            // Lazy init instead of open(): Flink 2.x replaced the
            // open(Configuration) signature with open(OpenContext).
            counters = new HashMap<>();
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
