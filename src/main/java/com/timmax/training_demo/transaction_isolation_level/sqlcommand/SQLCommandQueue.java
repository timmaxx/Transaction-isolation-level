package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElementType.*;
import static com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueState.*;

public class SQLCommandQueue {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommandQueue.class);

    private final Queue<SQLCommand> sqlCommandQueue = new LinkedList<>();
    private SQLCommandQueueState sqlCommandQueueState = IN_PREPARATION;
    private Thread thread;
    SQLCommandQueueLog sqlCommandQueueLog = new SQLCommandQueueLog();

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
                Optional<SQLCommandQueueLogElement> optionalSQLCommandQueueLogElement = sqlCommand.run();
                optionalSQLCommandQueueLogElement.ifPresent(
                        sqlCommandQueueLogElement -> sqlCommandQueueLog.push(sqlCommandQueueLogElement)
                );
            }
        });
        thread.setUncaughtExceptionHandler((t, throwable) -> {
            logger.error("Uncaught exception in thread = {}, throwable = {}", t, throwable.toString());
            rollback();
            sqlCommandQueueState = MALFUNCTIONED_ROLLED_BACK;
            try {
                throw throwable;
            } catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        });
        thread.start();
    }

    public void rollback() {
        if (thread.getState() != Thread.State.TERMINATED) {
            throw new UnsupportedOperationException();
        }
        while (true) {
            SQLCommandQueueLogElement sqlCommandQueueLogElement;
            try {
                sqlCommandQueueLogElement = sqlCommandQueueLog.pop();
            } catch (EmptyStackException ese) {
                break;
            }
            int rowId = sqlCommandQueueLogElement.getRowId();
            if (sqlCommandQueueLogElement.getSqlCommandQueueLogElementType() == INSERT) {
                sqlCommandQueueLogElement
                        .getBaseDbTable()
                        .rollback_delete(rowId);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        sqlCommandQueueState = ROLLED_BACK;
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
