package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.WhereFunc;

public class DQLCommandSelect extends DQLCommand {
    public DQLCommandSelect(DbTab dbTab) {
        this(0L, dbTab);
    }

    public DQLCommandSelect(DbTab dbTab, WhereFunc whereFunc) {
        this(0L, dbTab, whereFunc);
    }


    protected DQLCommandSelect(Long millsBeforeRun, DbTab dbTab) {
        this(millsBeforeRun, dbTab, dbRec -> true);
    }

    protected DQLCommandSelect(Long millsBeforeRun, DbTab dbTab, WhereFunc whereFunc) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.select(whereFunc);
    }
}
