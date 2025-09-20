package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;

public class TestData {
    public static final DbRecord recordForOneInsert = new DbRecord(123);
    public static final DbRecord recordAfterOneUpdate = new DbRecord(234);

    public static final ImmutableDbTable EMPTY_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB();
    public static final ImmutableDbTable ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB(recordForOneInsert);
    public static final ImmutableDbTable ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB(recordAfterOneUpdate);
}
