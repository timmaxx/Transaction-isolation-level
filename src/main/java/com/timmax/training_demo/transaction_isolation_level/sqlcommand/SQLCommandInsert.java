package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.SomeRecordInDB;
import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandInsert extends SQLCommand {
    protected SomeRecordInDB someRecordInDB;

    public SQLCommandInsert(BaseDbTable baseDbTable, SomeRecordInDB someRecordInDB) {
        super(baseDbTable);

        this.someRecordInDB = someRecordInDB;

        thread = new Thread(() -> {
            try {
                synchronized (this) {
                    logger.debug("Insert thread 1. Before sleep");
                    logger.debug("  baseDbTable =  {}", baseDbTable);
                }
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            synchronized (this) {
                logger.debug("Insert thread 2. After sleep and before insert");
                logger.debug("  baseDbTable =  {}", baseDbTable);
            }
            baseDbTable.insert(someRecordInDB);
            synchronized (this) {
                logger.debug("Insert thread 3. After insert");
                logger.debug("  baseDbTable =  {}", baseDbTable);
            }
        });
    }
}
