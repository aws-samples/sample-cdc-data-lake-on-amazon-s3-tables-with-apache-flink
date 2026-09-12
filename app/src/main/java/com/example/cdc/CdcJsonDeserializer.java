package com.example.cdc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Turns each Debezium change event into a self-contained JSON string the
 * {@link CdcDynamicRecordGenerator} can route and upsert.
 *
 * <p>The stock {@link JsonDebeziumDeserializationSchema} (which this class
 * delegates to for the value envelope) emits only the change envelope
 * ({@code before}/{@code after}/{@code source}/{@code op}) -- it does NOT carry
 * the row's primary key. For whole-schema sync we need the primary key of each
 * table to drive per-table upserts, so we read the PK column names from the
 * Kafka Connect message KEY schema ({@link SourceRecord#keySchema()}, which
 * Debezium populates with exactly the table's primary-key columns) and wrap
 * both together:
 *
 * <pre>{@code  {"__pk":["order_id"],"__t":{"dt":"ts-ms"},"e":<debezium-envelope-json>} }</pre>
 *
 * <p>{@code __t} maps column names to Debezium <b>semantic types</b> read from
 * the Kafka Connect value schema. The envelope alone encodes temporal columns
 * as bare numbers (DATETIME as epoch millis/micros, DATE as epoch days, TIME
 * as micros-of-day), indistinguishable from genuine integer columns once the
 * schema is gone. The generator uses these hints to land them as readable
 * temporal values instead of raw numbers (verified empirically: without the
 * hints a DATETIME(3) lands in Iceberg as {@code long 1789124400456}).
 *
 * Emitting a {@code String} keeps the stream element trivially serializable by
 * Flink (no custom TypeInformation, mirroring the reference sample's use of a
 * ready-made element type) and reuses Debezium's own, proven value converters.
 */
public final class CdcJsonDeserializer implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    // Reused for the value envelope. transient + lazy init: the stock schema is
    // not guaranteed serializable and there is no open() hook on this interface.
    private transient JsonDebeziumDeserializationSchema envelopeSchema;

    /** Debezium semantic schema name -> compact hint the generator understands. */
    private static String semanticHint(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        switch (schemaName) {
            case "io.debezium.time.Date":           return "date-days";
            case "io.debezium.time.Timestamp":      return "ts-ms";
            case "io.debezium.time.MicroTimestamp": return "ts-us";
            case "io.debezium.time.NanoTimestamp":  return "ts-ns";
            case "io.debezium.time.Time":           return "time-ms";
            case "io.debezium.time.MicroTime":      return "time-us";
            case "io.debezium.time.NanoTime":       return "time-ns";
            default:                                 return null;
        }
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        if (envelopeSchema == null) {
            envelopeSchema = new JsonDebeziumDeserializationSchema();
        }

        // Capture the Debezium value envelope as JSON via the stock schema.
        final String[] holder = new String[1];
        envelopeSchema.deserialize(record, new Collector<String>() {
            @Override
            public void collect(String s) {
                holder[0] = s;
            }

            @Override
            public void close() {
            }
        });
        final String envelopeJson = holder[0];
        if (envelopeJson == null) {
            return; // heartbeat / schema-change / tombstone with no value
        }

        // Primary-key column names from the Kafka Connect key schema.
        final StringBuilder pk = new StringBuilder();
        final Schema keySchema = record.keySchema();
        if (keySchema != null) {
            final List<Field> fields = keySchema.fields();
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    pk.append(',');
                }
                // Field names are simple SQL identifiers; JSON-quote them.
                pk.append('"').append(fields.get(i).name()).append('"');
            }
        }

        // Semantic-type hints from the Kafka Connect VALUE schema ("after" and
        // "before" share the table's field schemas; either carries the names).
        final StringBuilder hints = new StringBuilder();
        final Schema valueSchema = record.valueSchema();
        if (valueSchema != null && valueSchema.field("after") != null) {
            final Schema rowSchema = valueSchema.field("after").schema();
            if (rowSchema != null && rowSchema.fields() != null) {
                for (Field f : rowSchema.fields()) {
                    final String hint = semanticHint(f.schema().name());
                    if (hint != null) {
                        if (hints.length() > 0) {
                            hints.append(',');
                        }
                        hints.append('"').append(f.name()).append("\":\"").append(hint).append('"');
                    }
                }
            }
        }

        out.collect("{\"__pk\":[" + pk + "],\"__t\":{" + hints + "},\"e\":" + envelopeJson + "}");
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
