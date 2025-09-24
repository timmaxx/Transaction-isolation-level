package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.LogAndDataResultOfSQLCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class BaseDbTable {
    protected static final Logger logger = LoggerFactory.getLogger(BaseDbTable.class);

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

    public LogAndDataResultOfSQLCommand select(Integer rowId) {
        return new LogAndDataResultOfSQLCommand(Optional.empty(), select0(rowId));
    }

    private Optional<DbRecord> select0(Integer rowId) {
        return Optional.of(someRecordInDBMap.get(rowId));
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
                "rowId=" + lastInsertedRowId +
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
        return Objects.hash(lastInsertedRowId, someRecordInDBMap);
    }
}
