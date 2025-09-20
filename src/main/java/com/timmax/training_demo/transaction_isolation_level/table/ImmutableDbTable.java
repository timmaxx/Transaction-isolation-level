package com.timmax.training_demo.transaction_isolation_level.table;

public class ImmutableDbTable extends BaseDbTable {
    private ImmutableDbTable() {
        super();
    }

    private ImmutableDbTable(DbRecord dbRecord) {
        super();
        someRecordInDBMap.put(++rowId, dbRecord);
    }

    @Override
    public void insert(DbRecord dbRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSetField1EqualToField1Plus111(Integer rowId) {
        throw new UnsupportedOperationException();
    }

    public static ImmutableDbTable getImmutableTableInDB() {
        return new ImmutableDbTable();
    }

    public static ImmutableDbTable getImmutableTableInDB(DbRecord dbRecord) {
        return new ImmutableDbTable(dbRecord);
    }
}
