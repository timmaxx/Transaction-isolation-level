package com.timmax.training_demo.transaction_isolation_level.v02;

public non-sealed class DbSelect extends DbTableLike {
    public DbSelect(DbFields dbFields) {
        super(dbFields);
    }

    //  ToDo:   Возможно здесь метод даже через исключение можно не делать.
    @Override
    public void rollbackOfInsert(Integer rowId) {
        throw new UnsupportedOperationException();
    }

    //  ToDo:   Возможно здесь метод даже через исключение можно не делать.
    @Override
    public void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        throw new UnsupportedOperationException();
    }
}
