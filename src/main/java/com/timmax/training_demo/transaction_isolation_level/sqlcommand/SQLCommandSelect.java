package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

import java.util.Set;

public class SQLCommandSelect extends SQLCommand {
    public SQLCommandSelect(BaseDbTable baseDbTable, Set<Integer> rowIdSet) {
        this(0L, baseDbTable, rowIdSet);
    }

    public SQLCommandSelect(Long millsBeforeRun, BaseDbTable baseDbTable, Set<Integer> rowIdSet) {
        super(millsBeforeRun, baseDbTable);
        runnable = () -> baseDbTable.select(rowIdSet);
    }
}
