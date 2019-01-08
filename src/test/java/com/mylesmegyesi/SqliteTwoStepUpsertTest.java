package com.mylesmegyesi;

import org.sqlite.SQLiteException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.regex.Pattern;

class SqliteTwoStepUpsertTest extends BaseSqliteTest implements TwoStepUpsert.TwoStepUpsertStrategy {
    private static final String SQLITE_INSERT = "INSERT INTO people (email, name, created_at, updated_at) VALUES (?, ?, ?, ?)";
    private static final String SQLITE_UPDATE = "UPDATE people SET name = ?, updated_at = ? WHERE email = ? AND updated_at < ?";
    private static final Pattern SQLITE_DUPLICATE_EMAIL_PATTERN = Pattern
            .compile(".*UNIQUE constraint failed: people\\.email.*", Pattern.DOTALL);

    @Override
    UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        return TwoStepUpsert.upsert(this, connection, email, name, updatedAt);
    }

    public boolean insert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLITE_INSERT)) {
            long timestamp = updatedAt.toEpochMilli();
            statement.setString(1, email);
            statement.setString(2, name);
            statement.setLong(3, timestamp);
            statement.setLong(4, timestamp);
            try {
                statement.execute();
                return true;
            } catch (SQLiteException e) {
                if (isDuplicateEmailError(e)) {
                    return false;
                }

                throw e;
            }
        }
    }

    public UpsertResult update(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLITE_UPDATE)) {
            statement.setString(1, name);
            long timestamp = updatedAt.toEpochMilli();
            statement.setLong(2, timestamp);
            statement.setString(3, email);
            statement.setLong(4, timestamp);
            int numAffectedRows = statement.executeUpdate();
            if (numAffectedRows > 0) {
                return UpsertResult.Success;
            } else {
                return UpsertResult.StaleData;
            }
        }
    }

    private boolean isDuplicateEmailError(SQLiteException e) {
        return SQLITE_DUPLICATE_EMAIL_PATTERN.matcher(e.getMessage()).matches();
    }
}
