package com.aicampions.poker.context.adapter.snowflake;

import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Pattern;

/** Non-secret SPCS connection metadata; the rotating OAuth token remains in its runtime file. */
public record SnowflakeServiceCredentials(
        String account,
        String host,
        String warehouse,
        String database,
        String schema,
        Path tokenPath) {
    public static final String ACCOUNT_ENV = "SNOWFLAKE_ACCOUNT";
    public static final String HOST_ENV = "SNOWFLAKE_HOST";
    public static final String WAREHOUSE_ENV = "SNOWFLAKE_WAREHOUSE";
    public static final String DATABASE_ENV = "SNOWFLAKE_DATABASE";
    public static final String SCHEMA_ENV = "SNOWFLAKE_SCHEMA";
    public static final String TOKEN_PATH_ENV = "SNOWFLAKE_OAUTH_TOKEN_PATH";
    public static final String DEFAULT_TOKEN_PATH = "/snowflake/session/token";

    private static final Pattern HOST = Pattern.compile(
            "[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?");
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Z][A-Z0-9_$]*");

    public SnowflakeServiceCredentials {
        requireText(account, ACCOUNT_ENV);
        if (host == null || !HOST.matcher(host).matches()) {
            throw new IllegalArgumentException(HOST_ENV + " must be a DNS hostname");
        }
        requireIdentifier(warehouse, WAREHOUSE_ENV);
        requireIdentifier(database, DATABASE_ENV);
        requireIdentifier(schema, SCHEMA_ENV);
        if (tokenPath == null || !tokenPath.isAbsolute()) {
            throw new IllegalArgumentException(TOKEN_PATH_ENV + " must be an absolute path");
        }
    }

    public static SnowflakeServiceCredentials fromEnvironment(
            Map<String, String> environment) {
        return new SnowflakeServiceCredentials(
                environment.getOrDefault(ACCOUNT_ENV, ""),
                environment.getOrDefault(HOST_ENV, ""),
                environment.getOrDefault(WAREHOUSE_ENV, "DEMO_WH").toUpperCase(),
                environment.getOrDefault(DATABASE_ENV, "POKER_ML_DEMO").toUpperCase(),
                environment.getOrDefault(SCHEMA_ENV, "SPCS").toUpperCase(),
                Path.of(environment.getOrDefault(TOKEN_PATH_ENV, DEFAULT_TOKEN_PATH)));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required in SPCS");
        }
    }

    private static void requireIdentifier(String value, String name) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must be an unquoted Snowflake identifier");
        }
    }

    @Override
    public String toString() {
        return "SnowflakeServiceCredentials[service-identity,redacted-token]";
    }
}
