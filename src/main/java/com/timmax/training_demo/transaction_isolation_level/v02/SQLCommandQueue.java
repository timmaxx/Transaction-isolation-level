package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.ResultOfSQLCommand;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.*;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.DELETE;
import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.UPDATE;
import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.INSERT;

public class SQLCommandQueue {
    protected static final Logger logger = LoggerFactory.getLogger(SQLCommandQueue.class);

    private final Queue<DbTab.SQLCommand> sqlCommandQueue = new LinkedList<>();
    private Thread thread;
    //  !!! Нехорошо, что этот класс должен управлять экземплярами классов, которые лежат во вложенном пакете!!!
    DMLCommandQueueLog dmlCommandQueueLog = new DMLCommandQueueLog();
    //  !!! Нехорошо, что этот класс должен управлять экземплярами классов, которые лежат во вложенном пакете!!!
    DQLResultLog dqlResultLog = new DQLResultLog();

    private final AtomicReference<Throwable> exceptionRef = new AtomicReference<>(); // Для передачи исключения наружу

    public SQLCommandQueue() {
        super();
    }

    public SQLCommandQueue(DbTab.SQLCommand... sqlCommands) {
        this();
        add(sqlCommands);
    }

    public void add(DbTab.SQLCommand... sqlCommands) {
        Collections.addAll(sqlCommandQueue, sqlCommands);
    }

    public void startThread() {
        //  ToDo:   Следует сделать проверку, что если thread был определён и он в данный момент выполняется,
        //          то:
        //          - либо ждать здесь окончания его выполнения и потом определить новый,
        //          - либо выбросить исключение о том, что поток занят,
        //          - либо вернуть управление, но thread и так выполнит новые команды.

        thread = new Thread(() -> {
            //  В цикле не очищается очередь команд. Так не задумывалось изначально.
            for (DbTab.SQLCommand sqlCommand : sqlCommandQueue) {
                ResultOfSQLCommand resultOfSQLCommand;
                try {
                    resultOfSQLCommand = sqlCommand.run();
                } catch (DbSQLException dbSQLException) {
                    exceptionRef.set(dbSQLException);
                    //  !!!!!
                    // rollback();
                    throw dbSQLException;
                }

                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (resultOfSQLCommand instanceof ResultOfDMLCommand resultOfDMLCommand) {
                    dmlCommandQueueLog.push(resultOfDMLCommand.getDmlCommandLog());
                }
                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (resultOfSQLCommand instanceof ResultOfDQLCommand resultOfDQLCommand) {
                    dqlResultLog.push(resultOfDQLCommand.getDbSelect());
                }
            }
            //  Очистим очередь команд для возможного следующего использования объекта.
            sqlCommandQueue.clear();
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
        try {
            // Ждём завершения потока (в реальном коде это может быть join() с таймаутом)
            thread.join();
        } catch (InterruptedException interruptedException) {
            // Восстанавливаем прерванный статус
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptedException);
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

    public void rollback() {
        if (thread.getState() != Thread.State.TERMINATED) {
            throw new UnsupportedOperationException();
        }

        while (true) {
            DMLCommandLog dmlCommandLog;
            try {
                dmlCommandLog = dmlCommandQueueLog.pop();
            } catch (EmptyStackException ese) {
                break;
            }

            //  Отсюда и до конца тела while есть код, которого нет в commit
            DbTableLike dbTableLike = dmlCommandLog.getDbTabLike();
            DMLCommandLogElementType dmlCommandLogElementType = dmlCommandLog.getDmlCommandLogElementType();

            while (true) {
                DMLCommandLogElement dmlCommandLogElement;
                try {
                    dmlCommandLogElement = dmlCommandLog.pop();
                } catch (EmptyStackException ese) {
                    break;
                }
                int rowId = dmlCommandLogElement.getRowId();

                //  Вероятно этот if можно было-бы перенести в какой-нибудь класс - наследник.
                if (dmlCommandLogElementType == INSERT) {
                    //  для rollbackOfInsert нужен только rowId.
                    dbTableLike.rollbackOfInsert(rowId);
                } else if (dmlCommandLogElementType == UPDATE) {
                    //  для rollbackOfUpdate нужен и rowId и старое значение записи.
                    dbTableLike.rollbackOfUpdate(rowId, dmlCommandLogElement.getOldDbRec());
                } else if (dmlCommandLogElementType == DELETE) {
                    //  для rollbackOfDelete нужен и rowId и старое значение записи.
                    dbTableLike.rollbackOfDelete(rowId, dmlCommandLogElement.getOldDbRec());
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
    }

    public void commit() {
        if (thread.getState() != Thread.State.TERMINATED) {
            throw new UnsupportedOperationException();
        }

        while (true) {
            DMLCommandLog dmlCommandLog;
            try {
                dmlCommandLog = dmlCommandQueueLog.pop();
            } catch (EmptyStackException ese) {
                break;
            }

            //  Вызов метода, который очищает стек журнала отката
            dmlCommandLog.clear();
            //  Здесь нет кода, который есть в rollback
        }
    }
}
