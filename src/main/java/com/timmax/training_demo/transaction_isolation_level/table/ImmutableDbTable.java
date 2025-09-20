package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.SomeRecordInDB;

public class ImmutableDbTable extends BaseDbTable {
    private ImmutableDbTable() {
        super();
    }

    private ImmutableDbTable(SomeRecordInDB someRecordInDB) {
        super();
        someRecordInDBMap.put(++rowId, someRecordInDB);
    }

    @Override
    public void insert(SomeRecordInDB someRecordInDB) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSetField1EqualToField1Plus111(Integer rowId) {
        throw new UnsupportedOperationException();
    }

    public static ImmutableDbTable getImmutableTableInDB() {
        return new ImmutableDbTable();
    }

    public static ImmutableDbTable getImmutableTableInDB(SomeRecordInDB someRecordInDB) {
        return new ImmutableDbTable(someRecordInDB);
    }
}
