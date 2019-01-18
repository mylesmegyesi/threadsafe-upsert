package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

class PostgresInsertOnConflictUpsertTest extends BasePostgresTest {

  private static final String POSTGRES_UPSERT =
      "INSERT INTO people (email, name, created_at, updated_at) VALUES (?, ?, ?, ?) "
          + "ON CONFLICT (email) "
          + "DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at "
          + "WHERE EXCLUDED.updated_at > people.updated_at";

  @Override
  UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(POSTGRES_UPSERT)) {
      Timestamp timestamp = Timestamp.from(updatedAt);

      statement.setString(1, email);
      statement.setString(2, name);
      statement.setTimestamp(3, timestamp);
      statement.setTimestamp(4, timestamp);

      int numAffectedRows = statement.executeUpdate();
      if (numAffectedRows > 0) {
        return UpsertResult.Success;
      } else {
        return UpsertResult.StaleData;
      }
    }
  }
}
