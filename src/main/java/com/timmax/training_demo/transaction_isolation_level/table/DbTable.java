package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.SomeRecordInDB;

public class DbTable extends BaseDbTable {
    public DbTable(BaseDbTable baseDbTable) {
        super(baseDbTable);
    }

    @Override
    public void insert(SomeRecordInDB someRecordInDB) {
        logger.debug("i1 in thread");
        someRecordInDBMap.put(++rowId, someRecordInDB);
    }

    @Override
    public void updateSetField1EqualToField1Plus111(Integer rowId) {
        if (someRecordInDBMap.containsKey(rowId)) {
            int value = someRecordInDBMap.get(rowId).getField1();
            logger.debug("u1 in thread, value = {}", value);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.debug("u2 in thread, value = {}", value);
            someRecordInDBMap.put(rowId, new SomeRecordInDB(value + 111));
        }
    }
}
