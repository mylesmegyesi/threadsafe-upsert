package com.mylesmegyesi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.regex.Pattern;

class MySqlTwoStepUpsertTest extends BaseMysqlTest implements TwoStepUpsert.TwoStepUpsertStrategy {
    private static final String MYSQL_INSERT = "INSERT INTO people (email, name, created_at, updated_at) VALUES (?, ?, ?, ?)";
    private static final String MYSQL_UPDATE = "UPDATE people SET name = ?, updated_at = ? WHERE email = ? AND updated_at < ?";
    private static final Pattern MYSQL_DUPLICATE_EMAIL_PATTERN = Pattern
            .compile("^Duplicate entry '.*' for key 'email'$");

    @Override
    UpsertResult upsert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        return TwoStepUpsert.upsert(this, connection, email, name, updatedAt);
    }

    public boolean insert(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(MYSQL_INSERT)) {
            Timestamp timestamp = Timestamp.from(updatedAt);
            statement.setString(1, email);
            statement.setString(2, name);
            statement.setTimestamp(3, timestamp);
            statement.setTimestamp(4, timestamp);
            try {
                statement.execute();
                return true;
            } catch (SQLIntegrityConstraintViolationException e) {
                if (isDuplicateEmailError(e)) {
                    return false;
                }

                throw e;
            }
        }
    }

    public UpsertResult update(Connection connection, String email, String name, Instant updatedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(MYSQL_UPDATE)) {
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

    private boolean isDuplicateEmailError(SQLIntegrityConstraintViolationException e) {
        return MYSQL_DUPLICATE_EMAIL_PATTERN.matcher(e.getMessage()).matches();
    }
}
