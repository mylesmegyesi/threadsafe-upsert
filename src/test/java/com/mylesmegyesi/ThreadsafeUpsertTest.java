package com.mylesmegyesi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

class ThreadsafeUpsertTest {
    private static final String POSTGRES_USERNAME = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    private static final String POSTGRES_DATABASE_NAME = "threadsafe_upsert_test";
    private static final String POSTGRES_INSERT = "INSERT INTO people (id, email, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)";
    private static final String POSTGRES_UPDATE = "UPDATE people SET name = ?, updated_at = ? WHERE email = ? AND updated_at < ?";
    private static final String POSTGRES_CREATE_TABLE = "CREATE TABLE people (" +
            "id uuid PRIMARY KEY, " +
            "name varchar(255) NOT NULL, " +
            "email varchar(255) UNIQUE NOT NULL, " +
            "created_at timestamp with time zone, " +
            "updated_at timestamp with time zone" +
            ")";
    private static final Pattern POSTGRES_DUPLICATE_EMAIL_PATTERN = Pattern
            .compile(".*duplicate key value violates unique constraint \"people_email_key\".*", Pattern.DOTALL);

    private final Instant now = Instant.now();
    private final Instant yesterday = now.minus(Duration.ofDays(1));
    private final Properties props = getPostgresProperties();
    private Connection masterDatabaseConnection;

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

    private static <T> List<T> executeNTimesInParallel(int numThreads, int times, IntFunction<T> f) {
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            return IntStream.range(0, times)
                    .mapToObj(i -> threadPool.submit(() -> f.apply(i)))
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(toList());
        } finally {
            threadPool.shutdown();
        }
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
            UpsertResult result = upsert(connection, "j@j.com", "John", yesterday);
            assertThat(result, equalTo(UpsertResult.Success));

            List<Person> allPeople = fetchAllPeople(connection);

            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("John"));
            assertThat(person.getCreatedAt(), equalTo(yesterday));
            assertThat(person.getUpdatedAt(), equalTo(yesterday));
        }
    }

    @Test
    void updates_the_name_and_updatedAt_timestamp_if_the_email_already_exists() throws SQLException {
        try (Connection connection = getPostgresTestDatabaseConnection()) {
            Instant updatedAt = now.minus(Duration.ofDays(1));
            UpsertResult firstUpsertResult = upsert(connection, "j@j.com", "John", updatedAt);
            assertThat(firstUpsertResult, equalTo(UpsertResult.Success));

            UpsertResult secondUpsertResult = upsert(connection, "j@j.com", "Johnathon", now);
            assertThat(secondUpsertResult, equalTo(UpsertResult.Success));

            List<Person> allPeople = fetchAllPeople(connection);

            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("Johnathon"));
            assertThat(person.getCreatedAt(), equalTo(updatedAt));
            assertThat(person.getUpdatedAt(), equalTo(now));
        }
    }

    @Test
    void handles_many_writers_trying_to_update() throws SQLException {
        int numWriters = 100;
        Duration interval = Duration.between(yesterday, now).dividedBy(numWriters);
        List<UpsertResult> results = executeNTimesInParallel(8, numWriters, (i) -> {
            Instant updatedAt = yesterday.plus(interval.multipliedBy(i));
            try (Connection connection = getPostgresTestDatabaseConnection()) {
                return upsert(connection, "j@j.com", "John", updatedAt);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(results.size(), equalTo(numWriters));
        long numSuccessfulWrites = results.stream().filter(r -> r == UpsertResult.Success).count();
        assertThat(numSuccessfulWrites, greaterThanOrEqualTo(1L));

        try (Connection connection = getPostgresTestDatabaseConnection()) {
            List<Person> allPeople = fetchAllPeople(connection);
            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("John"));
            assertThat(person.getUpdatedAt(), equalTo(now.minus(interval)));
        }
    }

    @Test
    void handles_many_writers_trying_to_update_the_same_piece_of_data() throws SQLException {
        int numWriters = 100;
        List<UpsertResult> results = executeNTimesInParallel(8, numWriters, (i) -> {
            try (Connection connection = getPostgresTestDatabaseConnection()) {
                return upsert(connection, "j@j.com", "John", now);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(results.size(), equalTo(numWriters));
        long numSuccessfulWrites = results.stream().filter(r -> r == UpsertResult.Success).count();
        assertThat(numSuccessfulWrites, equalTo(1L));
        long numFailedWrites = results.stream().filter(r -> r == UpsertResult.StaleData).count();
        assertThat(numFailedWrites, equalTo(numWriters - 1L));

        try (Connection connection = getPostgresTestDatabaseConnection()) {
            List<Person> allPeople = fetchAllPeople(connection);
            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("John"));
            assertThat(person.getUpdatedAt(), equalTo(now));
        }
    }

    private UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try {
            return insert(connection, email, name, updatedAt);
        } catch (PSQLException e) {
            if (!isDuplicateEmailError(e)) {
                throw e;
            } else {
                return update(connection, email, name, updatedAt);
            }
        }
    }

    private UpsertResult insert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
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

    private UpsertResult update(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(POSTGRES_UPDATE)) {
            statement.setString(1, name);
            Timestamp timestamp = Timestamp.from(updatedAt);
            statement.setTimestamp(2, timestamp);
            statement.setString(3, email);
            statement.setTimestamp(4, timestamp);
            int numAffectedRows = statement.executeUpdate();
            if (numAffectedRows > 0) {
                return UpsertResult.Success;
            } else {
                return UpsertResult.StaleData;
            }
        }
    }

    private boolean isDuplicateEmailError(PSQLException e) {
        return POSTGRES_DUPLICATE_EMAIL_PATTERN.matcher(e.getMessage()).matches();
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
