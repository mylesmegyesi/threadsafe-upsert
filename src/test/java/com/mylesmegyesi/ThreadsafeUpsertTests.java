package com.mylesmegyesi;

import static com.mylesmegyesi.ConcurrencyUtils.executeNTimesInParallel;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

abstract class ThreadsafeUpsertTests<T> {
  private static final Instant NOW = Instant.now();
  private static final Instant YESTERDAY = NOW.minus(Duration.ofDays(1));
  private static final String SELECT_ALL_PEOPLE_QUERY = "SELECT * FROM people";
  private T database;

  @BeforeEach
  void before() throws SQLException, IOException {
    database = createTestDatabase();

    try (var testDatabaseConnection = getTestDatabaseConnection(database)) {
      try (var createTableStatement = testDatabaseConnection.createStatement()) {
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
    try (var connection = getTestDatabaseConnection(database)) {
      var result = upsert(connection, "j@j.com", "John", YESTERDAY);
      assertThat(result, equalTo(UpsertResult.Success));

      var allPeople = fetchAllPeople(connection);

      assertThat(allPeople, hasSize(1));
      var person = allPeople.get(0);
      assertThat(person.email, equalTo("j@j.com"));
      assertThat(person.name, equalTo("John"));
      assertThat(person.createdAt.toEpochMilli(), equalTo(YESTERDAY.toEpochMilli()));
      assertThat(person.updatedAt.toEpochMilli(), equalTo(YESTERDAY.toEpochMilli()));
    }
  }

  @Test
  void updates_the_name_and_updatedAt_timestamp_if_the_email_already_exists() throws SQLException {
    try (var connection = getTestDatabaseConnection(database)) {
      var updatedAt = NOW.minus(Duration.ofDays(1));
      var firstUpsertResult = upsert(connection, "j@j.com", "John", updatedAt);
      assertThat(firstUpsertResult, equalTo(UpsertResult.Success));

      var secondUpsertResult = upsert(connection, "j@j.com", "Johnathon", NOW);
      assertThat(secondUpsertResult, equalTo(UpsertResult.Success));

      var allPeople = fetchAllPeople(connection);

      assertThat(allPeople, hasSize(1));
      var person = allPeople.get(0);
      assertThat(person.email, equalTo("j@j.com"));
      assertThat(person.name, equalTo("Johnathon"));
      assertThat(person.createdAt.toEpochMilli(), equalTo(updatedAt.toEpochMilli()));
      assertThat(person.updatedAt.toEpochMilli(), equalTo(NOW.toEpochMilli()));
    }
  }

  @Test
  void handles_many_writers_trying_to_update() throws SQLException {
    var numWriters = 100;
    var interval = Duration.between(YESTERDAY, NOW).dividedBy(numWriters);
    var results =
        executeNTimesInParallel(
            8,
            numWriters,
            (index) -> {
              Instant updatedAt = YESTERDAY.plus(interval.multipliedBy(index));
              try (Connection connection = getTestDatabaseConnection(database)) {
                return upsert(connection, "j@j.com", "John", updatedAt);
              } catch (SQLException exception) {
                throw new RuntimeException(exception);
              }
            });

    assertThat(results.size(), equalTo(numWriters));
    var numSuccessfulWrites = results.stream().filter(r -> r == UpsertResult.Success).count();
    assertThat(numSuccessfulWrites, greaterThanOrEqualTo(1L));

    try (var connection = getTestDatabaseConnection(database)) {
      var allPeople = fetchAllPeople(connection);
      assertThat(allPeople, hasSize(1));
      var person = allPeople.get(0);
      assertThat(person.email, equalTo("j@j.com"));
      assertThat(person.name, equalTo("John"));
      assertThat(person.updatedAt.toEpochMilli(), equalTo(NOW.minus(interval).toEpochMilli()));
    }
  }

  @Test
  void handles_many_writers_trying_to_update_the_same_piece_of_data() throws SQLException {
    var numWriters = 100;
    var results =
        executeNTimesInParallel(
            8,
            numWriters,
            (index) -> {
              try (Connection connection = getTestDatabaseConnection(database)) {
                return upsert(connection, "j@j.com", "John", NOW);
              } catch (SQLException exception) {
                throw new RuntimeException(exception);
              }
            });

    assertThat(results.size(), equalTo(numWriters));
    var numSuccessfulWrites = results.stream().filter(r -> r == UpsertResult.Success).count();
    assertThat(numSuccessfulWrites, equalTo(1L));
    var numFailedWrites = results.stream().filter(r -> r == UpsertResult.StaleData).count();
    assertThat(numFailedWrites, equalTo(numWriters - 1L));

    try (var connection = getTestDatabaseConnection(database)) {
      var allPeople = fetchAllPeople(connection);
      assertThat(allPeople, hasSize(1));
      var person = allPeople.get(0);
      assertThat(person.email, equalTo("j@j.com"));
      assertThat(person.name, equalTo("John"));
      assertThat(person.updatedAt.toEpochMilli(), equalTo(NOW.toEpochMilli()));
    }
  }

  private List<Person> fetchAllPeople(Connection connection) throws SQLException {
    try (var statement = connection.createStatement()) {
      var resultSet = statement.executeQuery(SELECT_ALL_PEOPLE_QUERY);
      var people = new ArrayList<Person>();
      while (resultSet.next()) {
        people.add(
            new Person(
                resultSet.getString("email"),
                resultSet.getString("name"),
                getTimestamp(resultSet, "created_at"),
                getTimestamp(resultSet, "updated_at")));
      }
      return people;
    }
  }

  abstract T createTestDatabase() throws SQLException, IOException;

  abstract void dropTestDatabase(T database) throws SQLException;

  abstract String getCreateTableSql();

  abstract Connection getTestDatabaseConnection(T database) throws SQLException;

  abstract UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException;

  abstract Instant getTimestamp(ResultSet resultSet, String columnLabel) throws SQLException;
}
