package com.mylesmegyesi;

import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

abstract class BasePostgresTest extends ThreadsafeUpsertTests<Void> {

  private static final String POSTGRES_USERNAME = "postgres";
  private static final String POSTGRES_PASSWORD = "postgres";
  private static final String POSTGRES_DATABASE_NAME = "threadsafe_upsert_test";
  private static final String POSTGRES_CREATE_TABLE =
      "CREATE TABLE people ("
          + "id uuid PRIMARY KEY, "
          + "name varchar(255) NOT NULL, "
          + "email varchar(255) UNIQUE NOT NULL, "
          + "created_at timestamp with time zone, "
          + "updated_at timestamp with time zone"
          + ")";
  private static final Properties props = getPostgresProperties();
  private static final String POSTGRES_SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";

  private static Connection getMasterDatabaseConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:postgresql://localhost:9001/postgres", props);
  }

  private static Properties getPostgresProperties() {
    Properties props = new Properties();
    props.setProperty("user", POSTGRES_USERNAME);
    props.setProperty("password", POSTGRES_PASSWORD);
    return props;
  }

  @BeforeAll
  static void loadPostgresDatabaseDriver() throws ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
  }

  @Override
  Void createTestDatabase() throws SQLException {
    try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
      try (Statement createDatabaseStatement = masterDatabaseConnection.createStatement()) {
        createDatabaseStatement.executeUpdate("CREATE DATABASE " + POSTGRES_DATABASE_NAME);
      }
    }
    return null;
  }

  @Override
  String getCreateTableSql() {
    return POSTGRES_CREATE_TABLE;
  }

  @Override
  Connection getTestDatabaseConnection(Void database) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://localhost:9001/" + POSTGRES_DATABASE_NAME, props);
  }

  @Override
  void dropTestDatabase(Void database) throws SQLException {
    try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
      try (Statement dropDatabaseStatement = masterDatabaseConnection.createStatement()) {
        dropDatabaseStatement.executeUpdate("DROP DATABASE " + POSTGRES_DATABASE_NAME);
      }
    }
  }

  @Override
  List<Person> fetchAllPeople(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(POSTGRES_SELECT_ALL_PEOPLE_QUERY);
      List<Person> people = new ArrayList<>();
      while (resultSet.next()) {
        people.add(
            new Person(
                resultSet.getString("email"),
                resultSet.getString("name"),
                resultSet.getTimestamp("created_at").toInstant(),
                resultSet.getTimestamp("updated_at").toInstant()));
      }
      return people;
    }
  }
}
