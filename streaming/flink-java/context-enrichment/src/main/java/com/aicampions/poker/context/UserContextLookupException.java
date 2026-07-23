package com.aicampions.poker.context;

/** Sanitized failure used to trigger Flink checkpoint recovery. */
final class UserContextLookupException extends RuntimeException {
    UserContextLookupException(JdbcFailureClassifier.Failure failure) {
        super("user-context lookup failed [" + failure.safeCode() + "]");
    }
}
