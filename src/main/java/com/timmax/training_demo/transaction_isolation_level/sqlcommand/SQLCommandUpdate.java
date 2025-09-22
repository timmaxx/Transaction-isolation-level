package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.UpdateSetCalcFunc;

public class SQLCommandUpdate extends SQLCommand {
    public SQLCommandUpdate(BaseDbTable baseDbTable, Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        super(baseDbTable);

        runnable = () -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return baseDbTable.update(rowId, updateSetCalcFunc);
        };
    }
}
