package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.UpdateSetCalcFunc;

public class SQLCommandUpdate extends SQLCommand {
    public SQLCommandUpdate(BaseDbTable baseDbTable, Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        this(0L, 0L, baseDbTable, rowId, updateSetCalcFunc);
    }

    public SQLCommandUpdate(Long millsBeforeRun, Long millsInsideUpdate, BaseDbTable baseDbTable, Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        super(millsBeforeRun, baseDbTable);
        runnable = () -> baseDbTable.update(millsInsideUpdate, rowId, updateSetCalcFunc);
    }
}
