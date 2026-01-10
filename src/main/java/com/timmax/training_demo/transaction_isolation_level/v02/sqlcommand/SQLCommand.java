package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;

//  Будем считать, что SELECT относится к DQL (Data Query Language), но не к DML (Data Manipulation Language).
//  Вот иерархия для наследников:
//      -   DML команды (INSERT, UPDATE, DELETE)
//          -   могут изменять данные, а значит порождают журнал изменения,
//          -   не возвращают данные, а значит НЕ содержат результата.
//      -   DQL команда (SELECT)
//          -   не изменяет данные, а значит НЕ порождает журнал изменения,
//          -   возвращают данные, а значит содержит результат.
public abstract class SQLCommand {
    protected final DbTab dbTab;

    protected RunnableWithResultOfSQLCommand runnable;

    private final Long millsBeforeRun;


    public SQLCommand(DbTab dbTab) {
        this(0L, dbTab);
    }


    //  Done:   Для этого класса и его наследников, конструкторы с явным параметром
    //          Long millsBeforeRun
    //          лучше сделать не public (protected или package-private),
    //          т.к. в явном виде такие конструкторы нужны только для тестирования
    //          (особенно тестирование для разных уровней изоляции).
    protected SQLCommand(Long millsBeforeRun, DbTab dbTab) {
        this.dbTab = dbTab;
        this.millsBeforeRun = millsBeforeRun;
    }

    protected ResultOfSQLCommand run() {
        if (millsBeforeRun > 0) {
            try {
                Thread.sleep(millsBeforeRun);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return runnable.run();
    }
}
