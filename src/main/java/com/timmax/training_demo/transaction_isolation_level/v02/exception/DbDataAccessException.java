package com.timmax.training_demo.transaction_isolation_level.v02.exception;

public class DbDataAccessException extends DbSQLException {
    public DbDataAccessException(String reason) {
        super(reason);
    }
}
