package com.timmax.training_demo.transaction_isolation_level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SomeTableInDB {
    private static final Logger logger = LoggerFactory.getLogger(SomeTableInDB.class);

    private Integer rowId;
    private final Map<Integer, SomeRecordInDB> someRecordInDBMap;

    public SomeTableInDB() {
        rowId = 0;
        someRecordInDBMap = new HashMap<>();
    }

    public void insert(SomeRecordInDB someRecordInDB) {
        logger.debug("i1 in thread");
        someRecordInDBMap.put(++rowId, someRecordInDB);
    }

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

    @Override
    public String toString() {
        return "SomeTableInDB{" +
                "rowId=" + rowId +
                ", someRecordInDBMap=" + someRecordInDBMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SomeTableInDB that = (SomeTableInDB) o;
        return Objects.equals(rowId, that.rowId) && Objects.equals(someRecordInDBMap, that.someRecordInDBMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowId, someRecordInDBMap);
    }
}
