package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

class MySqlInsertOnDuplicateKeyUpsertTest extends BaseMysqlTest {

  private static final String MYSQL_UPSERT =
      "INSERT INTO people (email, name, created_at, updated_at) VALUES (?, ?, ?, ?) "
          + "ON DUPLICATE KEY UPDATE "
          + "name = IF(VALUES(updated_at) > people.updated_at, VALUES(name), name), "
          + "updated_at = IF("
          + "  VALUES(updated_at) > people.updated_at, "
          + "  VALUES(updated_at), "
          + "  updated_at"
          + ")";

  @Override
  UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt)
      throws SQLException {
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
}
