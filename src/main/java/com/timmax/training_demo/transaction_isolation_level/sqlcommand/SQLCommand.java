package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

import java.util.Optional;

public abstract class SQLCommand {
    protected final BaseDbTable baseDbTable;
    protected RunnableWithResultOptionalSQLCommandQueueLogElement runnable;

    public SQLCommand(BaseDbTable baseDbTable) {
        this.baseDbTable = baseDbTable;
    }

    Optional<SQLCommandQueueLogElement> run() {
        return runnable.run();
    }
}
