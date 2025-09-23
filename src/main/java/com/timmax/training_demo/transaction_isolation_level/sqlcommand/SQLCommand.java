package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public abstract class SQLCommand implements RunnableWithResultOptionalSQLCommandQueueLogElement {
    protected final BaseDbTable baseDbTable;
    protected RunnableWithResultOptionalSQLCommandQueueLogElement runnable;

    public SQLCommand(BaseDbTable baseDbTable) {
        this.baseDbTable = baseDbTable;
    }

    @Override
    public LogAndDataResultOfSQLCommand run() {
        return runnable.run();
    }
}
