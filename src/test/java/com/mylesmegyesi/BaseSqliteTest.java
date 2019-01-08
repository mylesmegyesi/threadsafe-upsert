package com.mylesmegyesi;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

abstract class BaseSqliteTest extends ThreadsafeUpsertTests<File> {

  private static final String SQLITE_CREATE_TABLE =
      "CREATE TABLE people ("
          + "name text NOT NULL, "
          + "email text UNIQUE NOT NULL, "
          + "created_at integer, "
          + "updated_at integer"
          + ")";
  private static final String SQLITE_SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";

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
  List<Person> fetchAllPeople(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(SQLITE_SELECT_ALL_PEOPLE_QUERY);
      List<Person> people = new ArrayList<>();
      while (resultSet.next()) {
        people.add(
            new Person(
                resultSet.getString("email"),
                resultSet.getString("name"),
                Instant.ofEpochMilli(resultSet.getLong("created_at")),
                Instant.ofEpochMilli(resultSet.getLong("updated_at"))));
      }
      return people;
    }
  }
}
