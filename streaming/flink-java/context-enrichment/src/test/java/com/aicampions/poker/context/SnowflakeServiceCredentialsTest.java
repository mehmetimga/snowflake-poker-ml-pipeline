package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aicampions.poker.context.adapter.snowflake.SnowflakeServiceCredentials;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SnowflakeServiceCredentialsTest {
    @Test
    void resolvesOnlySpcsMetadataAndKeepsTokenOutOfTheObject() {
        SnowflakeServiceCredentials credentials =
                SnowflakeServiceCredentials.fromEnvironment(
                        Map.of(
                                "SNOWFLAKE_ACCOUNT", "CLBSDFJ-BQ59861",
                                "SNOWFLAKE_HOST",
                                        "clbsdfj-bq59861.snowflakecomputing.com",
                                "SNOWFLAKE_WAREHOUSE", "demo_wh",
                                "SNOWFLAKE_DATABASE", "poker_ml_demo",
                                "SNOWFLAKE_SCHEMA", "spcs"));

        assertEquals("DEMO_WH", credentials.warehouse());
        assertEquals("POKER_ML_DEMO", credentials.database());
        assertEquals("SPCS", credentials.schema());
        assertEquals(
                "/snowflake/session/token",
                credentials.tokenPath().toString());
        assertFalse(credentials.toString().contains("clbsdfj"));
    }

    @Test
    void requiresTheSpcsProvidedAccountAndHost() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SnowflakeServiceCredentials.fromEnvironment(Map.of()));
        assertThrows(
                IllegalArgumentException.class,
                () -> SnowflakeServiceCredentials.fromEnvironment(
                        Map.of(
                                "SNOWFLAKE_ACCOUNT", "account",
                                "SNOWFLAKE_HOST",
                                        "https://unsafe.example.com")));
    }
}
