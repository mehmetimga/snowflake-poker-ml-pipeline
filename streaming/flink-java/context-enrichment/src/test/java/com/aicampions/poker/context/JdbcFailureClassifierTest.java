package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.aicampions.poker.context.adapter.jdbc.JdbcFailureClassifier;
import com.aicampions.poker.context.adapter.jdbc.UserContextLookupException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

final class JdbcFailureClassifierTest {
    @Test
    void classifiesSqlStateWithoutCopyingTheDatabaseMessage() {
        SQLException error = new SQLException(
                "connection to private-db.example failed for secret-user", "08006");
        JdbcFailureClassifier.Failure failure = JdbcFailureClassifier.classify(error);
        UserContextLookupException sanitized = new UserContextLookupException(failure);

        assertEquals(JdbcFailureClassifier.Kind.TRANSIENT, failure.kind());
        assertEquals("08", failure.sqlStateClass());
        assertFalse(sanitized.getMessage().contains("private-db.example"));
        assertFalse(sanitized.getMessage().contains("secret-user"));
    }

    @Test
    void distinguishesAuthorizationConfigurationAndDataFailures() {
        assertEquals(
                JdbcFailureClassifier.Kind.TRANSIENT,
                JdbcFailureClassifier.classify(
                        new SQLException("serialization retry", "40001"))
                        .kind());
        assertEquals(
                JdbcFailureClassifier.Kind.TRANSIENT,
                JdbcFailureClassifier.classify(
                        new SQLException("query canceled", "57014"))
                        .kind());
        assertEquals(
                JdbcFailureClassifier.Kind.AUTHENTICATION_OR_AUTHORIZATION,
                JdbcFailureClassifier.classify(new SQLException("denied", "28000")).kind());
        assertEquals(
                JdbcFailureClassifier.Kind.CONFIGURATION,
                JdbcFailureClassifier.classify(new SQLException("missing table", "42P01")).kind());
        assertEquals(
                JdbcFailureClassifier.Kind.DATA,
                JdbcFailureClassifier.classify(new SQLException("invalid value", "22000")).kind());
    }
}
