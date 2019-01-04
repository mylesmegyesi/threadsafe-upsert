package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public interface BaseMysqlTest extends ThreadsafeUpsertTests {
    String MYSQL_DATABASE_NAME = "threadsafe_upsert_test";
    String MYSQL_CREATE_TABLE = "CREATE TABLE people (" +
            "id serial, " +
            "name varchar(255) NOT NULL, " +
            "email varchar(255) UNIQUE NOT NULL, " +
            "created_at timestamp(6), " +
            "updated_at timestamp(6)" +
            ")";
    String MYSQL_SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";
    Properties MYSQL_PROPERTIES = getMysqlProperties();
    static Connection getMasterDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:9002/", MYSQL_PROPERTIES);
    }
    static Properties getMysqlProperties() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "password");
        props.setProperty("useSSL", "false");

        // This is necessary to make the affected-row count for ON DUPLICATE KEY UPDATE work correctly
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
        props.setProperty("useAffectedRows", "true");
        return props;
    }

    @Override
    default void createTestDatabase() throws SQLException {
        try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
            try (Statement createDatabaseStatement = masterDatabaseConnection.createStatement()) {
                createDatabaseStatement.executeUpdate("CREATE DATABASE " + MYSQL_DATABASE_NAME);
            }
        }
    }

    @Override
    default void dropTestDatabase() throws SQLException {
        try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
            try (Statement dropDatabaseStatement = masterDatabaseConnection.createStatement()) {
                dropDatabaseStatement.executeUpdate("DROP DATABASE " + MYSQL_DATABASE_NAME);
            }
        }
    }

    @Override
    default String getCreateTableSql() {
        return MYSQL_CREATE_TABLE;
    }

    @Override
    default Connection getTestDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:9002/" + MYSQL_DATABASE_NAME, MYSQL_PROPERTIES);
    }

    @Override
    default List<Person> fetchAllPeople(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(MYSQL_SELECT_ALL_PEOPLE_QUERY);
            List<Person> people = new ArrayList<>();
            while (resultSet.next()) {
                people.add(new Person(
                        resultSet.getString("email"),
                        resultSet.getString("name"),
                        resultSet.getTimestamp("created_at").toInstant(),
                        resultSet.getTimestamp("updated_at").toInstant()
                ));
            }
            return people;
        }
    }
}
