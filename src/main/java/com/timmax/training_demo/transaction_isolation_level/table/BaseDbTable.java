package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.LogAndDataResultOfSQLCommand;

import java.util.*;

public abstract class BaseDbTable {
    protected Integer lastInsertedRowId;
    protected final Map<Integer, DbRecord> someRecordInDBMap;

    public BaseDbTable() {
        this.someRecordInDBMap = new HashMap<>();
        this.lastInsertedRowId = 0;
    }

    public BaseDbTable(BaseDbTable baseDbTable) {
        this.lastInsertedRowId = baseDbTable.lastInsertedRowId;
        this.someRecordInDBMap = new HashMap<>(baseDbTable.someRecordInDBMap);
    }

    public int count() {
        return someRecordInDBMap.size();
    }

    public LogAndDataResultOfSQLCommand select(Set<Integer> rowIdSet) {
        return new LogAndDataResultOfSQLCommand(Optional.empty(), select0(rowIdSet));
    }

    private ImmutableDbTable select0(Set<Integer> rowIdSet) {
        Map<Integer, DbRecord> recordsInDBMap = new HashMap<>();
        for (Integer rowId : rowIdSet) {
            if (someRecordInDBMap.get(rowId) != null) {
                recordsInDBMap.put(rowId, someRecordInDBMap.get(rowId));
            }
        }
        return ImmutableDbTable.getImmutableTableInDB(recordsInDBMap);
    }

    public abstract LogAndDataResultOfSQLCommand insert(DbRecord newDbRecord);
    //  ToDo: Не должен быть public
    public abstract void rollback_insert(Integer rowId, DbRecord oldDbRecord);

    public abstract LogAndDataResultOfSQLCommand update(Integer rowId, UpdateSetCalcFunc updateSetCalcFunc);
    //  ToDo: Не должен быть public
    public abstract void rollback_update(Integer rowId, DbRecord oldDbRecord);

    public abstract LogAndDataResultOfSQLCommand delete(Integer rowId);
    //  ToDo: Не должен быть public
    public abstract void rollback_delete(Integer rowId);

    @Override
    public String toString() {
        return "BaseDbTable{" +
                "lastInsertedRowId=" + lastInsertedRowId +
                ", someRecordInDBMap=" + someRecordInDBMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BaseDbTable that)) return false;
        return Objects.equals(someRecordInDBMap, that.someRecordInDBMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(someRecordInDBMap);
    }
}
