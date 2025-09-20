package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.SomeTableInDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SQLCommand {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommand.class);

    protected int sessionId;
    protected final SomeTableInDB someTableInDB;
    protected Thread thread;

    public SQLCommand(int sessionId, SomeTableInDB someTableInDB) {
        this.sessionId = sessionId;
        this.someTableInDB = someTableInDB;
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
