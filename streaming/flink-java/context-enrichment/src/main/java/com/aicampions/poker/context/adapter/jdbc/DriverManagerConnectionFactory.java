package com.aicampions.poker.context.adapter.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** PostgreSQL DriverManager adapter with bounded connection and socket timeouts. */
final class DriverManagerConnectionFactory implements JdbcConnectionFactory {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int connectTimeoutSeconds;
    private final int socketTimeoutSeconds;

    DriverManagerConnectionFactory(
            String jdbcUrl,
            String username,
            String password,
            int connectTimeoutSeconds,
            int socketTimeoutSeconds) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.socketTimeoutSeconds = socketTimeoutSeconds;
    }

    @Override
    public Connection open() throws SQLException {
        if (!jdbcUrl.startsWith("jdbc:postgresql:")) {
            return DriverManager.getConnection(jdbcUrl, username, password);
        }
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty(
                "connectTimeout", Integer.toString(connectTimeoutSeconds));
        properties.setProperty(
                "socketTimeout", Integer.toString(socketTimeoutSeconds));
        properties.setProperty("tcpKeepAlive", "true");
        properties.setProperty(
                "ApplicationName", "poker-active-context-v2");
        return DriverManager.getConnection(jdbcUrl, properties);
    }
}
