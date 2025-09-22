package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

import static com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueState.*;

public class SQLCommandQueue {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommandQueue.class);

    private final Queue<SQLCommand> sqlCommandQueue = new LinkedList<>();
    private SQLCommandQueueState sqlCommandQueueState = IN_PREPARATION;
    private Thread thread;

    public void add(SQLCommand sqlCommand) {
        if (sqlCommandQueueState != IN_PREPARATION) {
            throw new UnsupportedOperationException();
        }
        sqlCommandQueue.add(sqlCommand);
    }

    public void startThread() {
        if (sqlCommandQueueState != IN_PREPARATION) {
            throw new UnsupportedOperationException();
        }
        sqlCommandQueueState = STARTED;
        thread = new Thread(() -> {
            for (SQLCommand sqlCommand : sqlCommandQueue) {
                sqlCommand.run();
            }
        });
        thread.setUncaughtExceptionHandler((t, throwable) -> {
            sqlCommandQueueState = MALFUNCTIONED_ROLLED_BACK;
            logger.error("Uncaught exception in thread = {}, throwable = {}", t, throwable.toString());
            try {
                throw throwable;
            } catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        });
        thread.start();
    }

    public void joinToThread() {
        if (sqlCommandQueueState != STARTED) {
            logger.info("sqlCommandQueueState = {}", sqlCommandQueueState);
            throw new UnsupportedOperationException();
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        sqlCommandQueueState = STOPPED;
    }
}
