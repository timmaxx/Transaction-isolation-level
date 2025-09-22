package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElement;

import java.util.Optional;

public class ImmutableDbTable extends BaseDbTable {
    private ImmutableDbTable() {
        super();
    }

    private ImmutableDbTable(DbRecord dbRecord) {
        super();
        someRecordInDBMap.put(++rowId, dbRecord);
    }

    @Override
    public Optional<SQLCommandQueueLogElement> insert(DbRecord newDbRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SQLCommandQueueLogElement> update(Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SQLCommandQueueLogElement> delete(Integer rowId) {
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
