package com.aicampions.poker.context;

import java.util.Map;

/** Credentials resolved only in the TaskManager process during operator open. */
record JdbcCredentials(String username, String password) {
    static final String USER_ENV = "USER_CONTEXT_DB_USER";
    static final String PASSWORD_ENV = "USER_CONTEXT_DB_PASSWORD";

    JdbcCredentials {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException(USER_ENV + " is required in the TaskManager");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException(PASSWORD_ENV + " is required in the TaskManager");
        }
    }

    static JdbcCredentials fromEnvironment(Map<String, String> environment) {
        return new JdbcCredentials(
                environment.getOrDefault(USER_ENV, ""),
                environment.getOrDefault(PASSWORD_ENV, ""));
    }

    @Override
    public String toString() {
        return "JdbcCredentials[redacted]";
    }
}
