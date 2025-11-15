package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.v02.DbSelect;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandQueueLogElement;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLResultLog;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.ResultOfDMLCommand;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.ResultOfDQLCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.Queue;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueueState.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandQueueLogElementType.*;

public class SQLCommandQueue {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommandQueue.class);

    private final Queue<SQLCommand> sqlCommandQueue = new LinkedList<>();
    private SQLCommandQueueState sqlCommandQueueState = IN_PREPARATION;
    private Thread thread;
    //  !!! Нехорошо, что этот класс должен управлять экземплярами классов, которые лежат во вложенном пакете!!!
    DMLCommandQueueLog dmlCommandQueueLog = new DMLCommandQueueLog();
    //  !!! Нехорошо, что этот класс должен управлять экземплярами классов, которые лежат во вложенном пакете!!!
    DQLResultLog dqlResultLog = new DQLResultLog();

    public SQLCommandQueue(SQLCommand... sqlCommands) {
        super();
        add(sqlCommands);
    }

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
                ResultOfSQLCommand resultOfSQLCommand = sqlCommand.run();
                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (resultOfSQLCommand instanceof ResultOfDMLCommand resultOfDMLCommand) {
                    dmlCommandQueueLog.push(resultOfDMLCommand.getLogResult());
                }
                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (resultOfSQLCommand instanceof ResultOfDQLCommand resultOfDQLCommand) {
                    dqlResultLog.push(resultOfDQLCommand.getDbSelect());
                }
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
            DMLCommandQueueLogElement dmlCommandQueueLogElement;
            try {
                dmlCommandQueueLogElement = dmlCommandQueueLog.pop();
            } catch (EmptyStackException ese) {
                break;
            }
            int rowId = dmlCommandQueueLogElement.getRowId();
            //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
            if (dmlCommandQueueLogElement.getDmlCommandqueuelogelementtype() == INSERT) {
/*
                dmlCommandQueueLogElement
                        .getDbTab()
                        .rollback_delete(rowId);
*/
            } else if (dmlCommandQueueLogElement.getDmlCommandqueuelogelementtype() == UPDATE) {
/*
                dmlCommandQueueLogElement
                        .getDbTab()
                        .rollback_update(rowId, dmlCommandQueueLogElement.oldDbRecord());
*/
            } else if (dmlCommandQueueLogElement.getDmlCommandqueuelogelementtype() == DELETE) {
/*
                dmlCommandQueueLogElement
                        .getDbTab()
                        .rollback_insert(rowId, dmlCommandQueueLogElement.oldDbRecord());
*/
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

    public DbSelect popFromDQLResultLog() {
        return dqlResultLog.pop();
    }
}
