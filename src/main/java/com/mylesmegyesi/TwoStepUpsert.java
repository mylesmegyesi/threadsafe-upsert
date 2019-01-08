package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

class TwoStepUpsert {
  static UpsertResult upsert(TwoStepUpsertStrategy strategy, Connection connection, String email, String name, Instant updatedAt) throws SQLException {
    if (strategy.insert(connection, email, name, updatedAt)) {
      return UpsertResult.Success;
    }

    return strategy.update(connection, email, name, updatedAt);
  }

  interface TwoStepUpsertStrategy {
    UpsertResult update(Connection connection, String email, String name, Instant updatedAt) throws SQLException;

    boolean insert(Connection connection, String email, String name, Instant updatedAt) throws SQLException;
  }
}
