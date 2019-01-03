package com.mylesmegyesi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

class ThreadsafeUpsertTest {
    private static final String POSTGRES_USERNAME = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    private static final String POSTGRES_DATABASE_NAME = "threadsafe_upsert_test";
    private static final String POSTGRES_INSERT = "INSERT INTO people (id, email, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)";
    private static final String POSTGRES_CREATE_TABLE = "CREATE TABLE people (" +
            "id uuid PRIMARY KEY, " +
            "name varchar(255) NOT NULL, " +
            "email varchar(255) UNIQUE NOT NULL, " +
            "created_at timestamp with time zone, " +
            "updated_at timestamp with time zone" +
            ")";

    private final Instant now = Instant.now();
    private final Properties props = getPostgresProperties();
    private Connection masterDatabaseConnection;

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

    @BeforeEach
    void before() throws SQLException {
        masterDatabaseConnection = DriverManager.getConnection("jdbc:postgresql://localhost:9001/postgres", props);

        try (Statement createDatabaseStatement = masterDatabaseConnection.createStatement()) {
            createDatabaseStatement.executeUpdate("CREATE DATABASE " + POSTGRES_DATABASE_NAME);
        }

        try (Connection testDatabaseConnection = getPostgresTestDatabaseConnection()) {
            try (Statement createTableStatement = testDatabaseConnection.createStatement()) {
                createTableStatement.executeUpdate(POSTGRES_CREATE_TABLE);
            }
        }
    }

    private Connection getPostgresTestDatabaseConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:9001/" + POSTGRES_DATABASE_NAME, props);
    }

    private Properties getPostgresProperties() {
        Properties props = new Properties();
        props.setProperty("user", POSTGRES_USERNAME);
        props.setProperty("password", POSTGRES_PASSWORD);
        return props;
    }

    @AfterEach
    void after() throws SQLException {
        try (Statement dropDatabaseStatement = masterDatabaseConnection.createStatement()) {
            dropDatabaseStatement.executeUpdate("DROP DATABASE " + POSTGRES_DATABASE_NAME);
        } finally {
            masterDatabaseConnection.close();
        }
    }

    @Test
    void creates_a_person_if_they_do_not_exist() throws SQLException {
        try (Connection connection = getPostgresTestDatabaseConnection()) {
            Instant updatedAt = now.minus(Duration.ofDays(1));
            UpsertResult result = upsert(connection, "j@j.com", "John", updatedAt);
            assertThat(result, equalTo(UpsertResult.Success));

            List<Person> allPeople = fetchAllPeople(connection);

            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("John"));
            assertThat(person.getCreatedAt(), equalTo(updatedAt));
            assertThat(person.getUpdatedAt(), equalTo(updatedAt));
        }
    }

    private UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(POSTGRES_INSERT)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, email);
            statement.setString(3, name);
            Timestamp timestamp = Timestamp.from(updatedAt);
            statement.setTimestamp(4, timestamp);
            statement.setTimestamp(5, timestamp);
            statement.execute();
            return UpsertResult.Success;
        }
    }

    private List<Person> fetchAllPeople(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM people");
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

    enum UpsertResult {
        Success,
        StaleData
    }

    class Person {
        private final String email;
        private final String name;
        private final Instant createdAt;
        private final Instant updatedAt;

        Person(String email, String name, Instant createdAt, Instant updatedAt) {
            this.email = email;
            this.name = name;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }

        String getEmail() {
            return email;
        }

        String getName() {
            return name;
        }

        Instant getCreatedAt() {
            return createdAt;
        }

        Instant getUpdatedAt() {
            return updatedAt;
        }
    }
}
