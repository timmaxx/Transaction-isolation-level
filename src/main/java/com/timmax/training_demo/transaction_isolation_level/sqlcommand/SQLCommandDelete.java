package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandDelete extends SQLCommand {
    public SQLCommandDelete(BaseDbTable baseDbTable, Integer rowId) {
        this(0L, baseDbTable, rowId);
    }

    public SQLCommandDelete(Long millsBeforeRun, BaseDbTable baseDbTable, Integer rowId) {
        super(millsBeforeRun, baseDbTable);
        runnable = () -> baseDbTable.delete(rowId);
    }
}
