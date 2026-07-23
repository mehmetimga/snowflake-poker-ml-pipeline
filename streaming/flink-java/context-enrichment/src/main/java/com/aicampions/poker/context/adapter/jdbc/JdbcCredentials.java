package com.aicampions.poker.context.adapter.jdbc;

import java.util.Map;

/** Credentials resolved only in the TaskManager process during operator open. */
public record JdbcCredentials(String username, String password) {
    public static final String USER_ENV = "USER_CONTEXT_DB_USER";
    public static final String PASSWORD_ENV = "USER_CONTEXT_DB_PASSWORD";

    public JdbcCredentials {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException(USER_ENV + " is required in the TaskManager");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException(PASSWORD_ENV + " is required in the TaskManager");
        }
    }

    public static JdbcCredentials fromEnvironment(Map<String, String> environment) {
        return new JdbcCredentials(
                environment.getOrDefault(USER_ENV, ""),
                environment.getOrDefault(PASSWORD_ENV, ""));
    }

    @Override
    public String toString() {
        return "JdbcCredentials[redacted]";
    }
}
