package com.mylesmegyesi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.mylesmegyesi.ConcurrencyUtils.executeNTimesInParallel;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public interface ThreadsafeUpsertTests {
    Instant now = Instant.now();
    Instant yesterday = now.minus(Duration.ofDays(1));

    @BeforeEach
    default void before() throws SQLException {
        createTestDatabase();

        try (Connection testDatabaseConnection = getTestDatabaseConnection()) {
            try (Statement createTableStatement = testDatabaseConnection.createStatement()) {
                createTableStatement.executeUpdate(getCreateTableSql());
            }
        }
    }

    @AfterEach
    default void after() throws SQLException {
        dropTestDatabase();
    }

    @Test
    default void creates_a_person_if_they_do_not_exist() throws SQLException {
        try (Connection connection = getTestDatabaseConnection()) {
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
    default void updates_the_name_and_updatedAt_timestamp_if_the_email_already_exists() throws SQLException {
        try (Connection connection = getTestDatabaseConnection()) {
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
    ;

    @Test
    default void handles_many_writers_trying_to_update() throws SQLException {
        int numWriters = 100;
        Duration interval = Duration.between(yesterday, now).dividedBy(numWriters);
        List<UpsertResult> results = executeNTimesInParallel(8, numWriters, (i) -> {
            Instant updatedAt = yesterday.plus(interval.multipliedBy(i));
            try (Connection connection = getTestDatabaseConnection()) {
                return upsert(connection, "j@j.com", "John", updatedAt);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(results.size(), equalTo(numWriters));
        long numSuccessfulWrites = results.stream().filter(r -> r == UpsertResult.Success).count();
        assertThat(numSuccessfulWrites, greaterThanOrEqualTo(1L));

        try (Connection connection = getTestDatabaseConnection()) {
            List<Person> allPeople = fetchAllPeople(connection);
            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("John"));
            assertThat(person.getUpdatedAt(), equalTo(now.minus(interval)));
        }
    }
    ;

    @Test
    default void handles_many_writers_trying_to_update_the_same_piece_of_data() throws SQLException {
        int numWriters = 100;
        List<UpsertResult> results = executeNTimesInParallel(8, numWriters, (i) -> {
            try (Connection connection = getTestDatabaseConnection()) {
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

        try (Connection connection = getTestDatabaseConnection()) {
            List<Person> allPeople = fetchAllPeople(connection);
            assertThat(allPeople, hasSize(1));
            Person person = allPeople.get(0);
            assertThat(person.getEmail(), equalTo("j@j.com"));
            assertThat(person.getName(), equalTo("John"));
            assertThat(person.getUpdatedAt(), equalTo(now));
        }
    }

    void createTestDatabase() throws SQLException;
    void dropTestDatabase() throws SQLException;
    String getCreateTableSql();
    Connection getTestDatabaseConnection() throws SQLException;
    UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException;
    List<Person> fetchAllPeople(Connection connection) throws SQLException;
}
