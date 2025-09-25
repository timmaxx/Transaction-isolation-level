package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

//  ToDo:   сделать рефакторинг, на основе нижеописанного.
//  Есть мнение, что SELECT относится к DQL (Data Query Language), но не к DML (Data Manipulation Language).
//  Я вижу, что:
//      -   DML команды (INSERT, UPDATE, DELETE)
//          -   могут изменять данные, а значит порождают журнал изменения,
//          -   не возвращают данные, а значит не содержат результата.
//      -   DQL команда (SELECT)
//          -   не изменяет данные, а значит НЕ порождает журнал изменения,
//          -   возвращают данные, а значит содержит результат.
//  Кроме того, сейчас имеется класс SQLCommandSleep, который вообще к SQL не относится.
public abstract class SQLCommand implements RunnableWithLogAndDataResultOfSQLCommand {
    protected final BaseDbTable baseDbTable;
    protected RunnableWithLogAndDataResultOfSQLCommand runnable;
    private final Long millsBeforeRun;

    public SQLCommand(Long millsBeforeRun, BaseDbTable baseDbTable) {
        this.baseDbTable = baseDbTable;
        this.millsBeforeRun = millsBeforeRun;
    }

    @Override
    public LogAndDataResultOfSQLCommand run() {
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
