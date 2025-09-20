package com.timmax.training_demo.transaction_isolation_level.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class BaseDbTable {
    protected static final Logger logger = LoggerFactory.getLogger(BaseDbTable.class);

    protected Integer rowId;
    protected final Map<Integer, DbRecord> someRecordInDBMap;

    public BaseDbTable() {
        this.someRecordInDBMap = new HashMap<>();
        this.rowId = 0;
    }

    public BaseDbTable(BaseDbTable baseDbTable) {
        this.rowId = baseDbTable.rowId;
        this.someRecordInDBMap = new HashMap<>(baseDbTable.someRecordInDBMap);
    }

    abstract public void insert(DbRecord dbRecord);

    abstract public void updateSetField1EqualToField1Plus111(Integer rowId);

    @Override
    public String toString() {
        return "BaseDbTable{" +
                "rowId=" + rowId +
                ", someRecordInDBMap=" + someRecordInDBMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BaseDbTable that)) return false;
        return Objects.equals(rowId, that.rowId) && Objects.equals(someRecordInDBMap, that.someRecordInDBMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowId, someRecordInDBMap);
    }
}
