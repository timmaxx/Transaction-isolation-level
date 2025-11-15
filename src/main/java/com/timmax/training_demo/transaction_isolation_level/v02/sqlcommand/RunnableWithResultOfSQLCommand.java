package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand;

@FunctionalInterface
public interface RunnableWithResultOfSQLCommand {
    ResultOfSQLCommand run();
}
