package com.timmax.training_demo.transaction_isolation_level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SomeTableInDB {
    private static final Logger logger = LoggerFactory.getLogger(SomeTableInDB.class);

    private Integer rowId;
    private final Map<Integer, SomeRecordInDB> someRecordInDBMap;

    public SomeTableInDB() {
        rowId = 0;
        someRecordInDBMap = new HashMap<>();
    }

    public void insert(SomeRecordInDB someRecordInDB) {
        logger.info("i1 in thread");
        someRecordInDBMap.put(++rowId, someRecordInDB);
    }

    public void updateSetField1EqualToField1Plus111(Integer rowId) {
        if (someRecordInDBMap.containsKey(rowId)) {
            int value = someRecordInDBMap.get(rowId).getField1();
            logger.info("u1 in thread, value = {}", value);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("u2 in thread, value = {}", value);
            someRecordInDBMap.put(rowId, new SomeRecordInDB(value + 111));
        }
    }

    @Override
    public String toString() {
        return "SomeTableInDB{" +
                "rowId=" + rowId +
                ", someRecordInDBMap=" + someRecordInDBMap +
                '}';
    }
}
