package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.SomeRecordInDB;
import com.timmax.training_demo.transaction_isolation_level.SomeTableInDB;

public class SQLCommandInsert extends SQLCommand {
    protected SomeRecordInDB someRecordInDB;

    public SQLCommandInsert(SomeTableInDB someTableInDB, SomeRecordInDB someRecordInDB) {
        super(someTableInDB);

        this.someRecordInDB = someRecordInDB;

        thread = new Thread(() -> {
            try {
                synchronized (this) {
                    logger.debug("Insert thread 1. Before sleep");
                    logger.debug("  someTableInDB =  {}", someTableInDB);
                }
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            synchronized (this) {
                logger.debug("Insert thread 2. After sleep and before insert");
                logger.debug("  someTableInDB =  {}", someTableInDB);
            }
            someTableInDB.insert(someRecordInDB);
            synchronized (this) {
                logger.debug("Insert thread 3. After insert");
                logger.debug("  someTableInDB =  {}", someTableInDB);
            }
        });
    }
}
