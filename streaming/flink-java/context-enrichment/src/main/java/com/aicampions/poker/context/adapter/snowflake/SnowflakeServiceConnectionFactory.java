package com.aicampions.poker.context.adapter.snowflake;

import com.aicampions.poker.context.adapter.jdbc.JdbcConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** Opens Snowflake JDBC sessions with the rotating OAuth token mounted by SPCS. */
public final class SnowflakeServiceConnectionFactory
        implements JdbcConnectionFactory {
    private final SnowflakeServiceCredentials credentials;
    private final int connectTimeoutSeconds;
    private final int networkTimeoutSeconds;

    public SnowflakeServiceConnectionFactory(
            SnowflakeServiceCredentials credentials,
            int connectTimeoutSeconds,
            int networkTimeoutSeconds)
            throws SQLException {
        this.credentials = credentials;
        this.connectTimeoutSeconds = positive(
                connectTimeoutSeconds, "connectTimeoutSeconds");
        this.networkTimeoutSeconds = positive(
                networkTimeoutSeconds, "networkTimeoutSeconds");
        try {
            Class.forName("net.snowflake.client.api.driver.SnowflakeDriver");
        } catch (ClassNotFoundException error) {
            throw new SQLException(
                    "Snowflake JDBC driver is unavailable", "08001", error);
        }
    }

    @Override
    public Connection open() throws SQLException {
        String token = readToken();
        Properties properties = new Properties();
        properties.setProperty("account", credentials.account());
        properties.setProperty("authenticator", "oauth");
        properties.setProperty("token", token);
        properties.setProperty("warehouse", credentials.warehouse());
        properties.setProperty("db", credentials.database());
        properties.setProperty("schema", credentials.schema());
        properties.setProperty(
                "loginTimeout", Integer.toString(connectTimeoutSeconds));
        properties.setProperty(
                "networkTimeout",
                Long.toString(networkTimeoutSeconds * 1_000L));
        properties.setProperty("queryTimeout", Integer.toString(networkTimeoutSeconds));
        properties.setProperty("CLIENT_SESSION_KEEP_ALIVE", "true");
        properties.setProperty("enablePutGet", "false");
        // SPCS already supplies the platform identity and Snowflake host.
        // Avoid AWS/Azure/GCP metadata probes that cannot succeed in the
        // container network and add noise and delay to every reconnect.
        properties.setProperty("disablePlatformDetection", "true");
        properties.setProperty("disableGcsDefaultCredentials", "true");
        return DriverManager.getConnection(
                "jdbc:snowflake://" + credentials.host() + "/",
                properties);
    }

    private String readToken() throws SQLException {
        try {
            String token = Files.readString(
                            credentials.tokenPath(), StandardCharsets.UTF_8)
                    .trim();
            if (token.isEmpty()) {
                throw new SQLException(
                        "SPCS service token file is empty", "28000");
            }
            return token;
        } catch (IOException error) {
            throw new SQLException(
                    "Unable to read the SPCS service token", "28000", error);
        }
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
