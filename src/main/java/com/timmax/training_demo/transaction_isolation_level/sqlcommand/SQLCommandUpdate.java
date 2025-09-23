package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.UpdateSetCalcFunc;

public class SQLCommandUpdate extends SQLCommand {
    public SQLCommandUpdate(BaseDbTable baseDbTable, Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        super(baseDbTable);

        runnable = () -> baseDbTable.update(rowId, updateSetCalcFunc);
    }
}
