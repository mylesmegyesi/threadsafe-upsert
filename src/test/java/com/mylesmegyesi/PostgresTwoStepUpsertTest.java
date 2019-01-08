package com.mylesmegyesi;

import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.regex.Pattern;

class PostgresTwoStepUpsertTest extends BasePostgresTest implements TwoStepUpsert.TwoStepUpsertStrategy {
  private static final String POSTGRES_INSERT = "INSERT INTO people (id, email, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)";
  private static final String POSTGRES_UPDATE = "UPDATE people SET name = ?, updated_at = ? WHERE email = ? AND updated_at < ?";
  private static final Pattern POSTGRES_DUPLICATE_EMAIL_PATTERN = Pattern
      .compile(".*duplicate key value violates unique constraint \"people_email_key\".*", Pattern.DOTALL);

  @Override
  UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
    return TwoStepUpsert.upsert(this, connection, email, name, updatedAt);
  }

  public boolean insert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(POSTGRES_INSERT)) {
      statement.setObject(1, UUID.randomUUID());
      statement.setString(2, email);
      statement.setString(3, name);
      Timestamp timestamp = Timestamp.from(updatedAt);
      statement.setTimestamp(4, timestamp);
      statement.setTimestamp(5, timestamp);
      try {
        statement.execute();
        return true;
      } catch (PSQLException e) {
        if (isDuplicateEmailError(e)) {
          return false;
        }

        throw e;
      }
    }
  }

  public UpsertResult update(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
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
}
