package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.SomeRecordInDB;
import com.timmax.training_demo.transaction_isolation_level.SomeTableInDB;

public class SQLCommandInsert extends SQLCommand {
    protected SomeRecordInDB someRecordInDB;

    public SQLCommandInsert(int sessionId, SomeTableInDB someTableInDB, SomeRecordInDB someRecordInDB) {
        super(sessionId, someTableInDB);

        this.someRecordInDB = someRecordInDB;

        thread = new Thread(() -> {
            try {
                logger.info("sessionId = {} i1 in thread", sessionId);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("sessionId = {} i2 in thread", sessionId);
            someTableInDB.insert(someRecordInDB);
            logger.info("sessionId = {} i3 in thread", sessionId);
        });
    }
}
