package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

class PostgresInsertOnConflictUpsertTest extends BasePostgresTest {

  private static final String POSTGRES_UPSERT =
      "INSERT INTO people (id, email, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT (email) "
          + "DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at "
          + "WHERE EXCLUDED.updated_at > people.updated_at";

  @Override
  UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(POSTGRES_UPSERT)) {
      Timestamp timestamp = Timestamp.from(updatedAt);

      statement.setObject(1, UUID.randomUUID());
      statement.setString(2, email);
      statement.setString(3, name);
      statement.setTimestamp(4, timestamp);
      statement.setTimestamp(5, timestamp);

      int numAffectedRows = statement.executeUpdate();
      if (numAffectedRows > 0) {
        return UpsertResult.Success;
      } else {
        return UpsertResult.StaleData;
      }
    }
  }
}
