package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.v02.DbSelect;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandQueueLog;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLResultLog;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.ResultOfDMLCommand;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.ResultOfDQLCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueueState.*;

public class SQLCommandQueue {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommandQueue.class);

    private final Queue<SQLCommand> sqlCommandQueue = new LinkedList<>();
    private SQLCommandQueueState sqlCommandQueueState = IN_PREPARATION;
    private Thread thread;
    //  !!! Нехорошо, что этот класс должен управлять экземплярами классов, которые лежат во вложенном пакете!!!
    DMLCommandQueueLog dmlCommandQueueLog = new DMLCommandQueueLog();
    //  !!! Нехорошо, что этот класс должен управлять экземплярами классов, которые лежат во вложенном пакете!!!
    DQLResultLog dqlResultLog = new DQLResultLog();

    private final AtomicReference<Throwable> exceptionRef = new AtomicReference<>(); // Для передачи исключения наружу

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
                ResultOfSQLCommand resultOfSQLCommand;
                try {
                    resultOfSQLCommand = sqlCommand.run();
                } catch (DbSQLException dbSQLException) {
                    exceptionRef.set(dbSQLException);
                    //  !!!!!
                    // rollback();
                    sqlCommandQueueState = MALFUNCTIONED_ROLLED_BACK;
                    throw dbSQLException;
                }

                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (resultOfSQLCommand instanceof ResultOfDMLCommand resultOfDMLCommand) {
                    dmlCommandQueueLog.push(resultOfDMLCommand.getResultLog());
                }
                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (resultOfSQLCommand instanceof ResultOfDQLCommand resultOfDQLCommand) {
                    dqlResultLog.push(resultOfDQLCommand.getDbSelect());
                }
            }
        });

        thread.setUncaughtExceptionHandler(
                (thread, throwable) -> {
                    if (!(throwable instanceof DbSQLException)) {
                        logger.error("Uncaught exception in thread = {}, throwable = {}", thread, throwable.toString());
                    }
                    exceptionRef.set(throwable);
                })
        ;

        thread.start();
    }

    public void joinToThread() {
        if (sqlCommandQueueState != STARTED) {
            logger.info("sqlCommandQueueState = {}", sqlCommandQueueState);
            throw new UnsupportedOperationException();
        }
        try {
            // Ждём завершения потока (в реальном коде это может быть join() с таймаутом)
            thread.join();
        } catch (InterruptedException interruptedException) {
            // Восстанавливаем прерванный статус
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptedException);
        } finally {
            sqlCommandQueueState = STOPPED;
        }

        // Если исключение было брошено, бросаем его заново в основной поток
        Throwable throwable = exceptionRef.get();
        if (throwable != null) {
            if (throwable instanceof RuntimeException rte) {
                throw rte;
            } else {
                throw new RuntimeException(throwable);
            }
        }
    }

    public DbSelect popFromDQLResultLog() {
        return dqlResultLog.pop();
    }
}

/*
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
                dmlCommandQueueLogElement
                        .getDbTab()
                        .rollback_delete(rowId);


            } else if (dmlCommandQueueLogElement.getDmlCommandqueuelogelementtype() == UPDATE) {
                dmlCommandQueueLogElement
                        .getDbTab()
                        .rollback_update(rowId, dmlCommandQueueLogElement.oldDbRecord());


            } else if (dmlCommandQueueLogElement.getDmlCommandqueuelogelementtype() == DELETE) {
                dmlCommandQueueLogElement
                        .getDbTab()
                        .rollback_insert(rowId, dmlCommandQueueLogElement.oldDbRecord());


            } else {
                throw new UnsupportedOperationException();
            }
        }
        sqlCommandQueueState = ROLLED_BACK;
    }
*/
