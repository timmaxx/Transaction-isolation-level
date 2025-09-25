package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.LogAndDataResultOfSQLCommand;

import java.util.Map;

public class ImmutableDbTable extends BaseDbTable {
    private ImmutableDbTable() {
        super();
    }

    private ImmutableDbTable(Map<Integer, DbRecord> integerDbRecordMap) {
        super();
        for (Map.Entry<Integer, DbRecord> integerDbRecordEntry : integerDbRecordMap.entrySet()) {
            if (integerDbRecordEntry.getValue() != null) {
                lastInsertedRowId = integerDbRecordEntry.getKey();
                someRecordInDBMap.put(lastInsertedRowId, integerDbRecordEntry.getValue());
            }
        }
    }

    @Override
    public LogAndDataResultOfSQLCommand insert(DbRecord newDbRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback_insert(Integer rowId, DbRecord oldDbRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogAndDataResultOfSQLCommand update(Long millsInsideUpdate, Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback_update(Integer rowId, DbRecord oldDbRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogAndDataResultOfSQLCommand delete(Integer rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback_delete(Integer rowId) {
        throw new UnsupportedOperationException();
    }

    public static ImmutableDbTable getImmutableTableInDB() {
        return new ImmutableDbTable();
    }

    public static ImmutableDbTable getImmutableTableInDB(Map<Integer, DbRecord> integerDbRecordMap) {
        return new ImmutableDbTable(integerDbRecordMap);
    }
}
