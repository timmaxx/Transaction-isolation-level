package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandInsert extends SQLCommand {
    public SQLCommandInsert(BaseDbTable baseDbTable, DbRecord newDbRecord) {
        this(0L, baseDbTable, newDbRecord);
    }

    public SQLCommandInsert(Long millsBeforeRun, BaseDbTable baseDbTable, DbRecord newDbRecord) {
        super(millsBeforeRun, baseDbTable);
        runnable = () -> baseDbTable.insert(newDbRecord);
    }
}
