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

public interface BasePostgresTest extends ThreadsafeUpsertTests {
    String POSTGRES_USERNAME = "postgres";
    String POSTGRES_PASSWORD = "postgres";
    String POSTGRES_DATABASE_NAME = "threadsafe_upsert_test";
    String POSTGRES_CREATE_TABLE = "CREATE TABLE people (" +
            "id uuid PRIMARY KEY, " +
            "name varchar(255) NOT NULL, " +
            "email varchar(255) UNIQUE NOT NULL, " +
            "created_at timestamp with time zone, " +
            "updated_at timestamp with time zone" +
            ")";
    Properties props = getPostgresProperties();
    String POSTGRES_SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";

    static Connection getMasterDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:9001/postgres", props);
    }

    static Properties getPostgresProperties() {
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
    default void createTestDatabase() throws SQLException {
        try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
            try (Statement createDatabaseStatement = masterDatabaseConnection.createStatement()) {
                createDatabaseStatement.executeUpdate("CREATE DATABASE " + POSTGRES_DATABASE_NAME);
            }
        }
    }

    @Override
    default String getCreateTableSql() {
        return POSTGRES_CREATE_TABLE;
    }

    @Override
    default Connection getTestDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:9001/" + POSTGRES_DATABASE_NAME, props);
    }

    @Override
    default void dropTestDatabase() throws SQLException {
        try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
            try (Statement dropDatabaseStatement = masterDatabaseConnection.createStatement()) {
                dropDatabaseStatement.executeUpdate("DROP DATABASE " + POSTGRES_DATABASE_NAME);
            }
        }
    }

    @Override
    default List<Person> fetchAllPeople(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(POSTGRES_SELECT_ALL_PEOPLE_QUERY);
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
