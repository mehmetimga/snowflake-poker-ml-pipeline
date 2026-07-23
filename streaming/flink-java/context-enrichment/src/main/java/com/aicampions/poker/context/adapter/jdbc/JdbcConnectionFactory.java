package com.aicampions.poker.context.adapter.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/** Test seam and runtime factory for one JDBC connection per Flink subtask. */
@FunctionalInterface
public interface JdbcConnectionFactory {
    Connection open() throws SQLException;
}
