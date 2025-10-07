package com.timmax.training_demo.transaction_isolation_level.v02.exception;

/*
* This class earlier was not needed because there is java.sql.SQLException.
* But that is simpler.
* */
public class DbSQLException extends RuntimeException {
    private String sqlState;
    private int vendorCode;

    public DbSQLException(String reason, String sqlState, int vendorCode) {
        super(reason);
        this.sqlState = sqlState;
        this.vendorCode = vendorCode;
    }

    public DbSQLException(String reason, String sqlState) {
        this(reason, sqlState, 0);
    }

    public DbSQLException(String reason) {
        this(reason, null);
    }
}
