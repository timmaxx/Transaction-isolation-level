package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommand;

//  DML команды (INSERT, UPDATE, DELETE)
//      -   могут изменять данные, а значит порождают журнал изменения,
//      -   не возвращают данные, а значит не содержат результата.
public abstract class DMLCommand extends SQLCommand {
    public DMLCommand(DbTab baseDbTable) {
        this(0L, baseDbTable);
    }


    protected DMLCommand(Long millsBeforeRun, DbTab baseDbTable) {
        super(millsBeforeRun, baseDbTable);
    }

    @Override
    protected final ResultOfDMLCommand run() {
        return (ResultOfDMLCommand)super.run();
    }
}
