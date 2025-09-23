package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

@FunctionalInterface
public interface RunnableWithResultOptionalSQLCommandQueueLogElement {
    /**
     * Runs this operation.
     */
    LogAndDataResultOfSQLCommand run();
}
