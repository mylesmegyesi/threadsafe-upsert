package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

abstract class BaseMysqlTest extends ThreadsafeUpsertTests<Void> {

  private static final String MYSQL_DATABASE_NAME = "threadsafe_upsert_test";
  private static final String MYSQL_CREATE_TABLE =
      "CREATE TABLE people ("
          + "name varchar(255) NOT NULL, "
          + "email varchar(255) UNIQUE NOT NULL, "
          + "created_at timestamp(6), "
          + "updated_at timestamp(6)"
          + ")";
  private static final String MYSQL_SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";
  private static final Properties MYSQL_PROPERTIES = getMysqlProperties();

  private static Connection getMasterDatabaseConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:mysql://localhost:9002/", MYSQL_PROPERTIES);
  }

  private static Properties getMysqlProperties() {
    Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", "password");
    props.setProperty("useSSL", "false");
    props.setProperty("allowPublicKeyRetrieval", "true");

    // This is necessary to make the affected-row count for ON DUPLICATE KEY UPDATE work correctly
    // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
    props.setProperty("useAffectedRows", "true");
    return props;
  }

  @Override
  Void createTestDatabase() throws SQLException {
    try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
      try (Statement createDatabaseStatement = masterDatabaseConnection.createStatement()) {
        createDatabaseStatement.executeUpdate("CREATE DATABASE " + MYSQL_DATABASE_NAME);
      }
    }
    return null;
  }

  @Override
  void dropTestDatabase(Void database) throws SQLException {
    try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
      try (Statement dropDatabaseStatement = masterDatabaseConnection.createStatement()) {
        dropDatabaseStatement.executeUpdate("DROP DATABASE " + MYSQL_DATABASE_NAME);
      }
    }
  }

  @Override
  String getCreateTableSql() {
    return MYSQL_CREATE_TABLE;
  }

  @Override
  Connection getTestDatabaseConnection(Void database) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:mysql://localhost:9002/" + MYSQL_DATABASE_NAME, MYSQL_PROPERTIES);
  }

  @Override
  List<Person> fetchAllPeople(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(MYSQL_SELECT_ALL_PEOPLE_QUERY);
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
