package com.aicampions.poker.context.adapter.jdbc;

/** Sanitized failure used to trigger Flink checkpoint recovery. */
public final class UserContextLookupException extends RuntimeException {
    public UserContextLookupException(JdbcFailureClassifier.Failure failure) {
        super("user-context lookup failed [" + failure.safeCode() + "]");
    }
}
