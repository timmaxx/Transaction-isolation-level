package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.WhereFunc;

public class DMLCommandDelete extends DMLCommand {
    public DMLCommandDelete(DbTab dbTab) {
        this(0L, dbTab, dbRec -> true);
    }

    public DMLCommandDelete(DbTab dbTab, WhereFunc whereFunc) {
        this(0L, dbTab, whereFunc);
    }


    protected DMLCommandDelete(Long millsBeforeRun, DbTab dbTab) {
        this(millsBeforeRun, dbTab, dbRec -> true);
    }

    protected DMLCommandDelete(Long millsBeforeRun, DbTab dbTab, WhereFunc whereFunc) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.delete(whereFunc);
    }
}
