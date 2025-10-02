package com.timmax.training_demo.transaction_isolation_level.v02;

import java.sql.SQLException;

public class DataAccessException extends SQLException {
    public DataAccessException(String reason) {
        super(reason);
    }
}
