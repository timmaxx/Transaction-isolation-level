package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SQLCommand {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommand.class);

    protected final BaseDbTable baseDbTable;
    protected Thread thread;

    public SQLCommand(BaseDbTable baseDbTable) {
        this.baseDbTable = baseDbTable;
    }

    public final void startThread() {
        thread.start();
    }

    public final void joinToThread() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
