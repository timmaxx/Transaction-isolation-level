package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

import java.util.Optional;

public class SQLCommandDelete extends SQLCommand {
    public SQLCommandDelete(BaseDbTable baseDbTable, Integer rowId) {
        super(baseDbTable);

        runnable = () -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            baseDbTable.delete(rowId);

            return Optional.empty();
        };
    }
}
