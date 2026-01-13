package com.timmax.training_demo.transaction_isolation_level.v02;

public non-sealed class DbSelect extends DbTableLike {
    public DbSelect(DbFields dbFields) {
        super(dbFields);
    }

    @Override
    void rollbackOfInsert(Integer rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        throw new UnsupportedOperationException();
    }

    @Override
    void rollbackOfUpdate(Integer rowId, DbRec oldDbRec) {
        throw new UnsupportedOperationException();
    }
}
