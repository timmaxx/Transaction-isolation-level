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

    void insert(int sessionId, SomeRecordInDB someRecordInDB) {
        new Thread(() -> {
            try {
                logger.info("sessionId = {} 1 in thread", sessionId);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("sessionId = {} 2 in thread", sessionId);
            someRecordInDBMap.put(++rowId, someRecordInDB);
            logger.info("sessionId = {} 3 in thread", sessionId);
        }).start();
    }

    void updateSetField1EqualToField1Plus111(int sessionId, Integer rowId) {
        new Thread(() -> {
            try {
                logger.info("sessionId = {} 1 in thread", sessionId);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (someRecordInDBMap.containsKey(rowId)) {
                // someRecordInDBMap.put(rowId, someRecordInDB);
                int value = someRecordInDBMap.get(rowId).getField1();
                logger.info("sessionId = {} 2 in thread, value = {}", sessionId, value);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                logger.info("sessionId = {} 2 in thread, value = {}", sessionId, value);
                someRecordInDBMap.put(rowId, new SomeRecordInDB(value + 111));
            }
        }).start();
    }

    @Override
    public String toString() {
        return "SomeTableInDB{" +
                "rowId=" + rowId +
                ", someRecordInDBMap=" + someRecordInDBMap +
                '}';
    }
}
