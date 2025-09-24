package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.LogAndDataResultOfSQLCommand;

public class ImmutableDbTable extends BaseDbTable {
    private ImmutableDbTable() {
        super();
    }

    private ImmutableDbTable(DbRecord dbRecord) {
        super();
        someRecordInDBMap.put(++lastInsertedRowId, dbRecord);
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
    public LogAndDataResultOfSQLCommand update(Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
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

    public static ImmutableDbTable getImmutableTableInDB(DbRecord dbRecord) {
        return new ImmutableDbTable(dbRecord);
    }
}
