package com.mylesmegyesi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.mylesmegyesi.ConcurrencyUtils.executeNTimesInParallel;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

abstract class ThreadsafeUpsertTests<T> {
  private static final Instant now = Instant.now();
  private static final Instant yesterday = now.minus(Duration.ofDays(1));
  private T database;

  @BeforeEach
  void before() throws SQLException, IOException {
    database = createTestDatabase();

    try (Connection testDatabaseConnection = getTestDatabaseConnection(database)) {
      try (Statement createTableStatement = testDatabaseConnection.createStatement()) {
        createTableStatement.executeUpdate(getCreateTableSql());
      }
    }
  }

  @AfterEach
  void after() throws SQLException {
    try {
      dropTestDatabase(database);
    } finally {
      database = null;
    }
  }

  @Test
  void creates_a_person_if_they_do_not_exist() throws SQLException {
    try (Connection connection = getTestDatabaseConnection(database)) {
      UpsertResult result = upsert(connection, "j@j.com", "John", yesterday);
      assertThat(result, equalTo(UpsertResult.Success));

      List<Person> allPeople = fetchAllPeople(connection);

      assertThat(allPeople, hasSize(1));
      Person person = allPeople.get(0);
      assertThat(person.getEmail(), equalTo("j@j.com"));
      assertThat(person.getName(), equalTo("John"));
      assertThat(person.getCreatedAt().toEpochMilli(), equalTo(yesterday.toEpochMilli()));
      assertThat(person.getUpdatedAt().toEpochMilli(), equalTo(yesterday.toEpochMilli()));
    }
  }

  @Test
  void updates_the_name_and_updatedAt_timestamp_if_the_email_already_exists() throws SQLException {
    try (Connection connection = getTestDatabaseConnection(database)) {
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
      assertThat(person.getCreatedAt().toEpochMilli(), equalTo(updatedAt.toEpochMilli()));
      assertThat(person.getUpdatedAt().toEpochMilli(), equalTo(now.toEpochMilli()));
    }
  }

  @Test
  void handles_many_writers_trying_to_update() throws SQLException {
    int numWriters = 100;
    Duration interval = Duration.between(yesterday, now).dividedBy(numWriters);
    List<UpsertResult> results = executeNTimesInParallel(8, numWriters, (i) -> {
      Instant updatedAt = yesterday.plus(interval.multipliedBy(i));
      try (Connection connection = getTestDatabaseConnection(database)) {
        return upsert(connection, "j@j.com", "John", updatedAt);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });

    assertThat(results.size(), equalTo(numWriters));
    long numSuccessfulWrites = results.stream().filter(r -> r == UpsertResult.Success).count();
    assertThat(numSuccessfulWrites, greaterThanOrEqualTo(1L));

    try (Connection connection = getTestDatabaseConnection(database)) {
      List<Person> allPeople = fetchAllPeople(connection);
      assertThat(allPeople, hasSize(1));
      Person person = allPeople.get(0);
      assertThat(person.getEmail(), equalTo("j@j.com"));
      assertThat(person.getName(), equalTo("John"));
      assertThat(person.getUpdatedAt().toEpochMilli(), equalTo(now.minus(interval).toEpochMilli()));
    }
  }

  @Test
  void handles_many_writers_trying_to_update_the_same_piece_of_data() throws SQLException {
    int numWriters = 100;
    List<UpsertResult> results = executeNTimesInParallel(8, numWriters, (i) -> {
      try (Connection connection = getTestDatabaseConnection(database)) {
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

    try (Connection connection = getTestDatabaseConnection(database)) {
      List<Person> allPeople = fetchAllPeople(connection);
      assertThat(allPeople, hasSize(1));
      Person person = allPeople.get(0);
      assertThat(person.getEmail(), equalTo("j@j.com"));
      assertThat(person.getName(), equalTo("John"));
      assertThat(person.getUpdatedAt().toEpochMilli(), equalTo(now.toEpochMilli()));
    }
  }

  abstract T createTestDatabase() throws SQLException, IOException;

  abstract void dropTestDatabase(T database) throws SQLException;

  abstract String getCreateTableSql();

  abstract Connection getTestDatabaseConnection(T database) throws SQLException;

  abstract UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException;

  abstract List<Person> fetchAllPeople(Connection connection) throws SQLException;
}
