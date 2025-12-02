package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbRec;
import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;

import java.util.List;

public class DMLCommandInsert extends DMLCommand {
    public DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, DbRec newDbRec) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.insert(newDbRec);
    }

    public DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, List<DbRec> newDbRec_List) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.insert(newDbRec_List);
    }
}
