package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbRec;
import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;

import java.util.List;

public class DMLCommandInsert extends DMLCommand {
    public DMLCommandInsert(DbTab dbTab, DbRec newDbRec) {
        this(0L, dbTab, newDbRec);
    }

    public DMLCommandInsert(DbTab dbTab, List<DbRec> newDbRec_List) {
        this(0L, dbTab, newDbRec_List);
    }


    protected DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, DbRec newDbRec) {
        this(millsBeforeRun, dbTab, List.of(newDbRec));
    }

    protected DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, List<DbRec> newDbRec_List) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.insert(newDbRec_List);
    }
}
