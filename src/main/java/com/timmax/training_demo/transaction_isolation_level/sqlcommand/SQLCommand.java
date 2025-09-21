package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public abstract class SQLCommand {
    protected final BaseDbTable baseDbTable;
    protected Runnable runnable;

    public SQLCommand(BaseDbTable baseDbTable) {
        this.baseDbTable = baseDbTable;
    }

    void run() {
        runnable.run();
    }
}
