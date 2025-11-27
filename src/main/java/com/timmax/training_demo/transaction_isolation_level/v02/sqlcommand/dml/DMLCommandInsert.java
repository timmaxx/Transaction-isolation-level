package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbRec;
import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;

public class DMLCommandInsert extends DMLCommand {
    public DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, DbRec newDbRec) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.insert(newDbRec);
    }
}
