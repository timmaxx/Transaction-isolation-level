package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;

public class SQLCommandInsert extends SQLCommand {
    protected DbRecord dbRecord;

    public SQLCommandInsert(BaseDbTable baseDbTable, DbRecord dbRecord) {
        super(baseDbTable);

        this.dbRecord = dbRecord;

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

            baseDbTable.insert(dbRecord);

            synchronized (this) {
                logger.debug("Insert thread 3. After insert");
                logger.debug("  baseDbTable =  {}", baseDbTable);
            }
        });
    }
}
