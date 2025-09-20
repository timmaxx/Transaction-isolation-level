package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.SomeRecordInDB;

public class TestData {
    public static final SomeRecordInDB recordForOneInsert = new SomeRecordInDB(123);

    public static final ImmutableDbTable EMPTY_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB();

    public static final ImmutableDbTable ONE_RECORD_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB(recordForOneInsert);
}
