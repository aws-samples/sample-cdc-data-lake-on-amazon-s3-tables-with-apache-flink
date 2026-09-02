package com.example.cdc;

import software.amazon.awssdk.protocols.jsoncore.JsonNode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.util.Optional;
import java.util.Properties;

/**
 * Resolves the CDC source database credentials from AWS Secrets Manager.
 *
 * <p>When the {@code cdc} property group carries a {@code secret-arn} key, the
 * secret it names is fetched once at job startup (using the MSF service
 * execution role via the default credentials chain) and its fields are
 * overlaid onto a copy of the property group. The database password then never
 * appears in the CloudFormation template, the MSF console, or
 * {@code DescribeApplication} output &mdash; only the secret's ARN does.
 *
 * <p>The secret value must be a JSON object using the standard RDS credential
 * key names. {@code username} and {@code password} are required; the optional
 * connection keys override their property-group counterparts when present:
 *
 * <pre>
 *   {"username": "cdc", "password": "...",           // required
 *    "host": "db.internal", "port": "3306",           // optional
 *    "dbname": "inventory"}                           // optional
 * </pre>
 *
 * <p>Without {@code secret-arn} the group is returned unchanged, so the local
 * Docker harness (no AWS) and existing plain username/password deployments
 * keep working exactly as before.
 *
 * <p>Rotation note: the secret is read once in {@code main()}. A rotated
 * password takes effect on the next application restart &mdash; the running
 * job keeps its already-established connection until then.
 */
final class DbSecrets {

    static final String SECRET_ARN_KEY = "secret-arn";

    private DbSecrets() {
    }

    /**
     * Returns the {@code cdc} property group with credentials resolved. If
     * {@code secret-arn} is absent this is the input, untouched; otherwise a
     * copy with {@code username}/{@code password} (and any optional connection
     * fields present in the secret) overlaid from Secrets Manager.
     */
    static Properties resolve(Properties cdc) {
        final String arn = cdc.getProperty(SECRET_ARN_KEY);
        if (arn == null || arn.isEmpty()) {
            return cdc;
        }
        final JsonNode secret = fetchSecretJson(arn);

        final Properties resolved = new Properties();
        for (String name : cdc.stringPropertyNames()) {
            resolved.setProperty(name, cdc.getProperty(name));
        }
        overlay(resolved, secret, arn, "username", "username", true);
        overlay(resolved, secret, arn, "password", "password", true);
        overlay(resolved, secret, arn, "host", "hostname", false);
        overlay(resolved, secret, arn, "port", "port", false);
        overlay(resolved, secret, arn, "dbname", "database-name", false);
        return resolved;
    }

    /** Fetch the secret string and parse it as a JSON object, failing fast. */
    private static JsonNode fetchSecretJson(String arn) {
        final String secretString;
        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .region(regionOf(arn))
                .build()) {
            secretString = client.getSecretValue(
                    GetSecretValueRequest.builder().secretId(arn).build())
                    .secretString();
        }
        if (secretString == null || secretString.isEmpty()) {
            throw new IllegalStateException(
                    "Secret '" + arn + "' has no SecretString. Store the database "
                    + "credentials as a JSON object ({\"username\": ..., "
                    + "\"password\": ...}), not as binary.");
        }
        final JsonNode node = JsonNode.parser().parse(secretString);
        if (!node.isObject()) {
            throw new IllegalStateException(
                    "Secret '" + arn + "' is not a JSON object. Expected the "
                    + "standard RDS credential shape: {\"username\": ..., "
                    + "\"password\": ...}.");
        }
        return node;
    }

    /**
     * Copy one secret field onto the properties. Required fields fail fast
     * naming the secret and the missing key; optional fields are skipped when
     * absent (their property-group value, if any, stays in effect).
     */
    private static void overlay(Properties target, JsonNode secret, String arn,
                                String secretKey, String propertyKey, boolean required) {
        final Optional<JsonNode> field = secret.field(secretKey);
        if (field.isEmpty()) {
            if (required) {
                throw new IllegalStateException(
                        "Secret '" + arn + "' is missing required key '" + secretKey
                        + "'. Store the database credentials as a JSON object with "
                        + "at least \"username\" and \"password\".");
            }
            return;
        }
        final JsonNode value = field.get();
        // RDS-style secrets store port as a number; everything downstream
        // wants strings, and asString() rejects non-string nodes.
        target.setProperty(propertyKey,
                value.isString() ? value.asString() : value.text());
    }

    /**
     * Derive the client region from the secret ARN itself
     * ({@code arn:<partition>:secretsmanager:<region>:...}) so the lookup
     * works regardless of environment, falling back to the MSF-provided
     * AWS_REGION / AWS_DEFAULT_REGION env vars.
     */
    private static Region regionOf(String arn) {
        final String[] parts = arn.split(":");
        if (parts.length > 3 && !parts[3].isEmpty()) {
            return Region.of(parts[3]);
        }
        final String env = CdcToIcebergJob.firstNonEmpty(
                System.getenv("AWS_REGION"), System.getenv("AWS_DEFAULT_REGION"));
        if (env != null) {
            return Region.of(env);
        }
        throw new IllegalStateException(
                "Could not resolve an AWS region for the Secrets Manager lookup: "
                + "'" + arn + "' is not a full secret ARN and neither AWS_REGION "
                + "nor AWS_DEFAULT_REGION is set.");
    }
}
