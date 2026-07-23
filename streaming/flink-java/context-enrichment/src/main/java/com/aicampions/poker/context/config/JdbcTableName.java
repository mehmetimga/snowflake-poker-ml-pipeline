package com.aicampions.poker.context.config;

import java.util.regex.Pattern;

/** Strict allow-list validation for the configured schema-qualified projection table. */
public final class JdbcTableName {
    private static final Pattern SAFE_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    private JdbcTableName() {}

    public static String validate(String tableName) {
        if (tableName == null || !SAFE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException(
                    "invalid user-context table name");
        }
        return tableName;
    }
}
