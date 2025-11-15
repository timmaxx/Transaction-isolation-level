package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.RunnableWithResultOfSQLCommand;

@FunctionalInterface
public interface RunnableWithResultOfDQLCommand extends RunnableWithResultOfSQLCommand {
    ResultOfDQLCommand run();
}
