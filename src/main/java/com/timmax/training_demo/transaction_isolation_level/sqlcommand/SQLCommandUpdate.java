package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.SomeTableInDB;

public class SQLCommandUpdate extends SQLCommand {
    public SQLCommandUpdate(int sessionId, SomeTableInDB someTableInDB) {
        super(sessionId, someTableInDB);

        thread = new Thread(() -> {
            try {
                logger.info("sessionId = {} u1 in thread", sessionId);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            someTableInDB.updateSetField1EqualToField1Plus111(1);
        });
    }
}
