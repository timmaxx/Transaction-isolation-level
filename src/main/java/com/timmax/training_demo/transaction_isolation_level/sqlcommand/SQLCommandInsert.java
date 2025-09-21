package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandInsert extends SQLCommand {
    protected DbRecord dbRecord;

    public SQLCommandInsert(BaseDbTable baseDbTable, DbRecord dbRecord) {
        super(baseDbTable);

        this.dbRecord = dbRecord;

        runnable = () -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            baseDbTable.insert(dbRecord);
        };
    }
}
