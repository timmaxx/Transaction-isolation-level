package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.UpdateSetCalcFunc;
import com.timmax.training_demo.transaction_isolation_level.v02.WhereFunc;

public class DMLCommandUpdate extends DMLCommand {
    public DMLCommandUpdate(Long millsBeforeRun, DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc) {
        this(millsBeforeRun, dbTab, updateSetCalcFunc, null);
    }

    public DMLCommandUpdate(Long millsBeforeRun, DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        super(millsBeforeRun, dbTab);
        runnable = () -> dbTab.update(updateSetCalcFunc, whereFunc);
    }
}
