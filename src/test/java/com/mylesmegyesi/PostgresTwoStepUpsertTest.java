package com.mylesmegyesi;

import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.regex.Pattern;

class PostgresTwoStepUpsertTest extends BasePostgresTest
    implements TwoStepUpsert.TwoStepUpsertStrategy {

  private static final String POSTGRES_INSERT =
      "INSERT INTO people (email, name, created_at, updated_at) VALUES (?, ?, ?, ?)";

  private static final String POSTGRES_UPDATE =
      "UPDATE people SET name = ?, updated_at = ? WHERE email = ? AND updated_at < ?";

  private static final Pattern POSTGRES_DUPLICATE_EMAIL_PATTERN =
      Pattern.compile(
          ".*duplicate key value violates unique constraint \"people_email_key\".*",
          Pattern.DOTALL);

  @Override
  UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException {
    return TwoStepUpsert.upsert(this, connection, email, name, updatedAt);
  }

  public boolean insert(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException {
    try (var statement = connection.prepareStatement(POSTGRES_INSERT)) {
      var timestamp = Timestamp.from(updatedAt);

      statement.setString(1, email);
      statement.setString(2, name);
      statement.setTimestamp(3, timestamp);
      statement.setTimestamp(4, timestamp);
      try {
        statement.execute();
        return true;
      } catch (PSQLException exception) {
        if (isDuplicateEmailError(exception)) {
          return false;
        }

        throw exception;
      }
    }
  }

  public UpsertResult update(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException {
    try (var statement = connection.prepareStatement(POSTGRES_UPDATE)) {
      statement.setString(1, name);
      var timestamp = Timestamp.from(updatedAt);
      statement.setTimestamp(2, timestamp);
      statement.setString(3, email);
      statement.setTimestamp(4, timestamp);
      var numAffectedRows = statement.executeUpdate();
      if (numAffectedRows > 0) {
        return UpsertResult.Success;
      } else {
        return UpsertResult.StaleData;
      }
    }
  }

  private boolean isDuplicateEmailError(PSQLException exception) {
    return POSTGRES_DUPLICATE_EMAIL_PATTERN.matcher(exception.getMessage()).matches();
  }
}
