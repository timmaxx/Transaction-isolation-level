package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommand;

//  DQL команда (SELECT)
//      -   не изменяет данные, а значит НЕ порождает журнал изменения,
//      -   возвращают данные, а значит содержит результат.
public abstract class DQLCommand extends SQLCommand {
    public DQLCommand(DbTab dbTab) {
        this(0L, dbTab);
    }


    protected DQLCommand(Long millsBeforeRun, DbTab dbTab) {
        super(millsBeforeRun, dbTab);
    }

    @Override
    protected final ResultOfDQLCommand run() {
        return (ResultOfDQLCommand)super.run();
    }
}
