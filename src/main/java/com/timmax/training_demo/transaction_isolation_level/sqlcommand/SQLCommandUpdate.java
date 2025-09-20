package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandUpdate extends SQLCommand {
    public SQLCommandUpdate(BaseDbTable baseDbTable) {
        super(baseDbTable);

        thread = new Thread(() -> {
            try {
                logger.debug("Update thread 1. Before sleep");
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            baseDbTable.updateSetField1EqualToField1Plus111(1);
        });
    }
}
