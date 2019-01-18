package com.mylesmegyesi;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

abstract class BaseSqliteTest extends ThreadsafeUpsertTests<File> {
  private static final String SQLITE_CREATE_TABLE =
      "CREATE TABLE people ("
          + "name text NOT NULL, "
          + "email text UNIQUE NOT NULL, "
          + "created_at integer, "
          + "updated_at integer"
          + ")";

  @Override
  File createTestDatabase() throws IOException {
    return File.createTempFile("sqlite-", "-test-db");
  }

  @Override
  void dropTestDatabase(File database) {
    database.delete();
  }

  @Override
  String getCreateTableSql() {
    return SQLITE_CREATE_TABLE;
  }

  @Override
  Connection getTestDatabaseConnection(File database) throws SQLException {
    return DriverManager.getConnection("jdbc:sqlite:" + database.getAbsolutePath());
  }

  @Override
  Instant getTimestamp(ResultSet resultSet, String columnLabel) throws SQLException {
    return Instant.ofEpochMilli(resultSet.getLong(columnLabel));
  }
}
