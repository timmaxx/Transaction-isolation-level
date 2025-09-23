package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

import java.util.Optional;

public class SQLCommandSleep extends SQLCommand {
    //  ToDo:   Удалить использование baseDbTable.
    public SQLCommandSleep(BaseDbTable baseDbTable, long millis) {
        super(null);

        runnable = () -> {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return new LogAndDataResultOfSQLCommand(Optional.empty(), Optional.empty());
        };
    }
}
