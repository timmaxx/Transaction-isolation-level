package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.RunnableWithResultOfSQLCommand;

@FunctionalInterface
public interface RunnableWithResultOfDMLCommand extends RunnableWithResultOfSQLCommand {
    ResultOfDMLCommand run();
}
