package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.Queue;

import static com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElementType.*;
import static com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueState.*;

public class SQLCommandQueue {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommandQueue.class);

    private final Queue<SQLCommand> sqlCommandQueue = new LinkedList<>();
    private SQLCommandQueueState sqlCommandQueueState = IN_PREPARATION;
    private Thread thread;
    SQLCommandQueueLog sqlCommandQueueLog = new SQLCommandQueueLog();
    ImmutableDbTableResultLog immutableDbTableResultLog = new ImmutableDbTableResultLog();

    public void add(SQLCommand... sqlCommands) {
        if (sqlCommandQueueState != IN_PREPARATION) {
            throw new UnsupportedOperationException();
        }
        Collections.addAll(sqlCommandQueue, sqlCommands);
    }

    public void startThread() {
        if (sqlCommandQueueState != IN_PREPARATION) {
            throw new UnsupportedOperationException();
        }
        sqlCommandQueueState = STARTED;
        thread = new Thread(() -> {
            for (SQLCommand sqlCommand : sqlCommandQueue) {
                LogAndDataResultOfSQLCommand logAndDataResultOfSQLCommand = sqlCommand.run();

                logAndDataResultOfSQLCommand.logResult().ifPresent(
                        sqlCommandQueueLogElement -> sqlCommandQueueLog.push(sqlCommandQueueLogElement)
                );
                immutableDbTableResultLog.push(logAndDataResultOfSQLCommand.dqlResult());
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
            int rowId = sqlCommandQueueLogElement.rowId();
            if (sqlCommandQueueLogElement.sqlCommandQueueLogElementType() == INSERT) {
                sqlCommandQueueLogElement
                        .baseDbTable()
                        .rollback_delete(rowId);
            } else if (sqlCommandQueueLogElement.sqlCommandQueueLogElementType() == UPDATE) {
                sqlCommandQueueLogElement
                        .baseDbTable()
                        .rollback_update(rowId, sqlCommandQueueLogElement.oldDbRecord());
            } else if (sqlCommandQueueLogElement.sqlCommandQueueLogElementType() == DELETE) {
                sqlCommandQueueLogElement
                        .baseDbTable()
                        .rollback_insert(rowId, sqlCommandQueueLogElement.oldDbRecord());
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

    public ImmutableDbTable popFromImmutableDbTableResultLog() {
        return immutableDbTableResultLog.pop();
    }
}
