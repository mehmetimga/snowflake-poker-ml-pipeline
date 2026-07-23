package com.aicampions.poker.context;

import java.sql.SQLException;

/** Maps JDBC failures to bounded, non-sensitive operational categories. */
final class JdbcFailureClassifier {
    enum Kind {
        TRANSIENT("transient"),
        AUTHENTICATION_OR_AUTHORIZATION("authentication-or-authorization"),
        CONFIGURATION("configuration"),
        DATA("data"),
        UNKNOWN("unknown");

        private final String code;

        Kind(String code) {
            this.code = code;
        }

        String code() {
            return code;
        }
    }

    record Failure(Kind kind, String sqlStateClass) {
        String safeCode() {
            return "jdbc-" + kind.code() + "-sqlstate-" + sqlStateClass;
        }
    }

    private JdbcFailureClassifier() {}

    static Failure classify(Throwable error) {
        SQLException sqlError = findSqlException(error);
        if (sqlError == null) {
            return new Failure(Kind.UNKNOWN, "none");
        }
        String state = sqlError.getSQLState();
        if (state == null || state.length() < 2) {
            return new Failure(Kind.UNKNOWN, "none");
        }
        String stateClass = state.substring(0, 2);
        Kind kind = switch (stateClass) {
            case "08", "40", "53", "55", "57", "58" -> Kind.TRANSIENT;
            case "28" -> Kind.AUTHENTICATION_OR_AUTHORIZATION;
            case "0A", "3D", "3F", "42" -> Kind.CONFIGURATION;
            case "22", "23" -> Kind.DATA;
            default -> Kind.UNKNOWN;
        };
        return new Failure(kind, stateClass);
    }

    private static SQLException findSqlException(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 8; depth++) {
            if (current instanceof SQLException sqlException) {
                return sqlException;
            }
            current = current.getCause();
        }
        return null;
    }
}
