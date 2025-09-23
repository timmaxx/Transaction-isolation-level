package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

@FunctionalInterface
public interface RunnableWithLogAndDataResultOfSQLCommand {
    /**
     * Runs this operation.
     */
    LogAndDataResultOfSQLCommand run();
}
