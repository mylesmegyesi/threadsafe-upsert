package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MysqlInsertOnDuplicateKeyUpsertTest implements ThreadsafeUpsertTests {
    private static final String MYSQL_DATABASE_NAME = "threadsafe_upsert_test";
    private static final String MYSQL_CREATE_TABLE = "CREATE TABLE people (" +
            "id serial, " +
            "name varchar(255) NOT NULL, " +
            "email varchar(255) UNIQUE NOT NULL, " +
            "created_at timestamp(6), " +
            "updated_at timestamp(6)" +
            ")";
    private static final String MYSQL_SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";
    private static final String MYSQL_UPSERT = "INSERT INTO people (email, name, created_at, updated_at) VALUES (?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "name = IF(VALUES(updated_at) > people.updated_at, VALUES(name), name), " +
            "updated_at = IF(VALUES(updated_at) > people.updated_at, VALUES(updated_at), updated_at) ";
    private static final Properties MYSQL_PROPERTIES = getMysqlProperties();

    private static Connection getMasterDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:9002/", MYSQL_PROPERTIES);
    }

    private static Properties getMysqlProperties() {
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
    public void createTestDatabase() throws SQLException {
        try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
            try (Statement createDatabaseStatement = masterDatabaseConnection.createStatement()) {
                createDatabaseStatement.executeUpdate("CREATE DATABASE " + MYSQL_DATABASE_NAME);
            }
        }
    }

    @Override
    public void dropTestDatabase() throws SQLException {
        try (Connection masterDatabaseConnection = getMasterDatabaseConnection()) {
            try (Statement dropDatabaseStatement = masterDatabaseConnection.createStatement()) {
                dropDatabaseStatement.executeUpdate("DROP DATABASE " + MYSQL_DATABASE_NAME);
            }
        }
    }

    @Override
    public String getCreateTableSql() {
        return MYSQL_CREATE_TABLE;
    }

    @Override
    public Connection getTestDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:9002/" + MYSQL_DATABASE_NAME, MYSQL_PROPERTIES);
    }

    @Override
    public UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(MYSQL_UPSERT)) {
            Timestamp timestamp = Timestamp.from(updatedAt);

            statement.setString(1, email);
            statement.setString(2, name);
            statement.setTimestamp(3, timestamp);
            statement.setTimestamp(4, timestamp);

            int numAffectedRows = statement.executeUpdate();
            if (numAffectedRows == 0) {
                return UpsertResult.StaleData;
            } else {
                return UpsertResult.Success;
            }
        }
    }

    @Override
    public List<Person> fetchAllPeople(Connection connection) throws SQLException {
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
