package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbRec;
import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommand;

public class DMLCommandInsert extends SQLCommand {
    public DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, DbRec newDbRec) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.insert(newDbRec);
    }
}
