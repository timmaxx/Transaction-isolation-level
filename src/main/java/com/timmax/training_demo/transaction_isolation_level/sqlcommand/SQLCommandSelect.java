package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandSelect extends SQLCommand {
    public SQLCommandSelect(BaseDbTable baseDbTable, Integer rowId) {
        super(baseDbTable);

        runnable = () -> baseDbTable.select(rowId);
    }
}
