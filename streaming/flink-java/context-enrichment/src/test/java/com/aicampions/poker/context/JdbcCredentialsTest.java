package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

final class JdbcCredentialsTest {
    @Test
    void resolvesOnlyAtRuntimeAndRedactsItsStringForm() {
        JdbcCredentials credentials = JdbcCredentials.fromEnvironment(Map.of(
                JdbcCredentials.USER_ENV, "context_reader",
                JdbcCredentials.PASSWORD_ENV, "secret-value"));

        assertEquals("context_reader", credentials.username());
        assertEquals("secret-value", credentials.password());
        assertEquals("JdbcCredentials[redacted]", credentials.toString());
    }

    @Test
    void requiresBothTaskManagerSecretValues() {
        assertThrows(
                IllegalArgumentException.class,
                () -> JdbcCredentials.fromEnvironment(Map.of()));
    }
}
