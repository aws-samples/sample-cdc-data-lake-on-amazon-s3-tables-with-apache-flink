package com.example.cdc;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes each Debezium change event (wrapped by {@link CdcJsonDeserializer} as
 * {@code {"__pk":[...],"e":<envelope>}}) to an Iceberg table
 * {@code <namespace>.<source_table>}, inferring the table schema from the row
 * and upserting on the source primary key.
 *
 * <p>The schema-inference and JSON&rarr;RowData conversion below are copied
 * (scalar subset + string fallback) from the reference sample's
 * {@code SchemaAgnosticRoutingGenerator}
 * (aws-samples/sample-streaming-data-lake-with-apache-iceberg-and-apache-flink,
 * dynamic-sink-sample). Only the CDC-specific parts are new:
 * <ul>
 *   <li>routing by the Debezium {@code source.table};</li>
 *   <li>{@link RowKind} from the Debezium {@code op} (delete uses {@code before},
 *       everything else uses {@code after});</li>
 *   <li>per-record upsert mode + equality fields = the source primary key.</li>
 * </ul>
 *
 * <p>Deliberate simplification vs the reference (the blog is a pass-through sync
 * of relational rows, "no transforms"): only scalar column types are modelled
 * (string / long / double / boolean, plus ISO timestamp/date detection); any
 * non-scalar value falls back to its string form, and partitioning is dropped
 * (unpartitioned tables). Because the stock Debezium JSON converter renders
 * temporal and decimal columns as epoch numbers / strings, those land as
 * long/double/string here -- exactly as they would in the reference's
 * JSON-type-based inference. Exact Iceberg temporal/decimal types would require
 * a schema-carrying deserializer, which is out of scope for the minimal demo.
 */
public final class CdcDynamicRecordGenerator implements DynamicRecordGenerator<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CdcDynamicRecordGenerator.class);

    private final String namespace;
    @SuppressWarnings("unused") // reserved: propagate to table props if needed
    private final String formatVersion;

    // Stable field IDs across schema variations for a given column name.
    private final Map<String, Integer> fieldIdRegistry = new ConcurrentHashMap<>();
    private int nextFieldId = 1;
    // Schema cache keyed by table + sorted field signature.
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();

    private transient ObjectMapper mapper;

    public CdcDynamicRecordGenerator(String namespace, String formatVersion) {
        this.namespace = namespace;
        this.formatVersion = formatVersion;
    }

    @Override
    public void open(OpenContext openContext) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public void generate(String wrapperJson, Collector<DynamicRecord> out) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        final JsonNode wrapper = mapper.readTree(wrapperJson);
        final JsonNode envelope = wrapper.get("e");
        if (envelope == null || !envelope.isObject()) {
            return;
        }

        final String op = text(envelope.get("op"));
        final JsonNode source = envelope.get("source");
        final String table = source != null ? text(source.get("table")) : null;
        if (table == null || table.isEmpty()) {
            LOG.warn("CDC record without source.table, skipping: {}", wrapperJson);
            return;
        }

        final boolean isDelete = "d".equals(op);
        // c/r/u -> use "after"; d -> use "before".
        final JsonNode payload = isDelete ? envelope.get("before") : envelope.get("after");
        if (payload == null || !payload.isObject()) {
            return; // e.g. truncate / DDL / unsupported op with no row image
        }

        final Set<String> pk = readPrimaryKey(wrapper.get("__pk"));

        // A delete with no primary key cannot be applied as an equality delete.
        if (isDelete && pk.isEmpty()) {
            LOG.warn("Delete on table '{}' has no primary key; skipping (cannot upsert-delete)", table);
            return;
        }

        final TableIdentifier tableId = TableIdentifier.of(namespace, table);
        final String signature = schemaSignature(table, payload);
        final Schema schema = schemaCache.computeIfAbsent(signature, sig -> inferSchema(payload));

        final RowData rowData = toRowData(payload, schema, isDelete ? RowKind.DELETE : RowKind.INSERT);

        final DynamicRecord record = new DynamicRecord(
                tableId,
                "main",
                schema,
                rowData,
                PartitionSpec.unpartitioned(), // pass-through: no partition transforms
                DistributionMode.NONE,
                1);

        if (!pk.isEmpty()) {
            // Upsert on the source primary key. Non-delete rows become
            // insert-or-update; delete rows (RowKind.DELETE) become equality
            // deletes on these columns.
            record.setUpsertMode(true);
            record.setEqualityFields(pk);
        }

        out.collect(record);
    }

    private Set<String> readPrimaryKey(JsonNode pkNode) {
        final Set<String> pk = new LinkedHashSet<>();
        if (pkNode != null && pkNode.isArray()) {
            for (JsonNode n : pkNode) {
                final String name = text(n);
                if (name != null && !name.isEmpty()) {
                    pk.add(name);
                }
            }
        }
        return pk;
    }

    // ---- schema inference (copied from the reference, scalar subset) --------

    private String schemaSignature(String table, JsonNode json) {
        final List<String> sigs = new ArrayList<>();
        final Iterator<String> names = json.fieldNames();
        while (names.hasNext()) {
            final String name = names.next();
            sigs.add(name + "=" + typeCode(json.get(name)));
        }
        sigs.sort(Comparator.naturalOrder());
        return table + ":" + String.join(",", sigs);
    }

    private String typeCode(JsonNode v) {
        if (v == null || v.isNull()) {
            return "null";
        }
        if (v.isTextual()) {
            final String t = v.asText();
            if (isTimestamp(t)) {
                return "timestamp";
            }
            if (isDate(t)) {
                return "date";
            }
            return "string";
        }
        // Widen integrals -> long, fractionals -> double so the same logical
        // column never yields divergent signatures.
        if (v.isNumber()) {
            return (v.isFloatingPointNumber() || v.isBigDecimal()) ? "double" : "long";
        }
        if (v.isBoolean()) {
            return "boolean";
        }
        return "string"; // arrays/objects fall back to string
    }

    private Schema inferSchema(JsonNode json) {
        final List<Types.NestedField> fields = new ArrayList<>();
        final Iterator<String> names = json.fieldNames();
        while (names.hasNext()) {
            final String name = names.next();
            final int id = fieldId(name);
            // All optional: safest for schema evolution as new columns appear.
            fields.add(Types.NestedField.optional(id, name, icebergType(json.get(name))));
        }
        fields.sort(Comparator.comparingInt(Types.NestedField::fieldId));
        return new Schema(fields);
    }

    private Type icebergType(JsonNode v) {
        if (v == null || v.isNull()) {
            return Types.StringType.get();
        }
        if (v.isTextual()) {
            final String t = v.asText();
            if (isTimestamp(t)) {
                return Types.TimestampType.withZone();
            }
            if (isDate(t)) {
                return Types.DateType.get();
            }
            return Types.StringType.get();
        }
        if (v.isNumber()) {
            return (v.isFloatingPointNumber() || v.isBigDecimal())
                    ? Types.DoubleType.get()
                    : Types.LongType.get();
        }
        if (v.isBoolean()) {
            return Types.BooleanType.get();
        }
        return Types.StringType.get(); // arrays/objects -> string form
    }

    private synchronized int fieldId(String name) {
        return fieldIdRegistry.computeIfAbsent(name, n -> nextFieldId++);
    }

    // ---- JSON -> RowData (copied from the reference, scalar subset) ----------

    private RowData toRowData(JsonNode json, Schema schema, RowKind kind) {
        final GenericRowData row = new GenericRowData(kind, schema.columns().size());
        int i = 0;
        for (Types.NestedField field : schema.columns()) {
            row.setField(i++, convert(json.get(field.name()), field.type()));
        }
        return row;
    }

    private Object convert(JsonNode v, Type type) {
        if (v == null || v.isNull()) {
            return null;
        }
        if (type instanceof Types.StringType) {
            return StringData.fromString(v.isValueNode() ? v.asText() : v.toString());
        }
        if (type instanceof Types.LongType) {
            return v.asLong();
        }
        if (type instanceof Types.DoubleType) {
            return v.asDouble();
        }
        if (type instanceof Types.BooleanType) {
            return v.asBoolean();
        }
        if (type instanceof Types.TimestampType) {
            if (v.isTextual()) {
                return TimestampData.fromInstant(Instant.parse(v.asText()));
            }
            if (v.isNumber()) {
                return TimestampData.fromEpochMillis(v.asLong());
            }
            return null;
        }
        if (type instanceof Types.DateType) {
            if (v.isTextual()) {
                return (int) LocalDate.parse(v.asText()).toEpochDay();
            }
            if (v.isNumber()) {
                return v.asInt();
            }
            return null;
        }
        return StringData.fromString(v.isValueNode() ? v.asText() : v.toString());
    }

    // ---- small helpers -------------------------------------------------------

    private static String text(JsonNode n) {
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private boolean isTimestamp(String text) {
        if (text == null || text.length() < 19 || !text.contains("T")) {
            return false;
        }
        try {
            Instant.parse(text);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private boolean isDate(String text) {
        if (text == null || text.length() != 10) {
            return false;
        }
        try {
            LocalDate.parse(text);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
