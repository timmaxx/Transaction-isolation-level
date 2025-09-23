package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public abstract class SQLCommand implements RunnableWithLogAndDataResultOfSQLCommand {
    protected final BaseDbTable baseDbTable;
    protected RunnableWithLogAndDataResultOfSQLCommand runnable;

    public SQLCommand(BaseDbTable baseDbTable) {
        this.baseDbTable = baseDbTable;
    }

    @Override
    public LogAndDataResultOfSQLCommand run() {
        return runnable.run();
    }
}
