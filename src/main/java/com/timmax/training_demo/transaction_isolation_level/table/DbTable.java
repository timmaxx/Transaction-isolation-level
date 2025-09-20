package com.timmax.training_demo.transaction_isolation_level.table;

public class DbTable extends BaseDbTable {
    public DbTable(BaseDbTable baseDbTable) {
        super(baseDbTable);
    }

    @Override
    public void insert(DbRecord dbRecord) {
        logger.debug("i1 in thread");
        someRecordInDBMap.put(++rowId, dbRecord);
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
            someRecordInDBMap.put(rowId, new DbRecord(value + 111));
        }
    }
}
