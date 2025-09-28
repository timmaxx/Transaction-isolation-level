package com.timmax.training_demo.transaction_isolation_level.v01;

import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;

import java.util.Map;

public class TestData {
    public static final DbRecord recordForOneInsert = new DbRecord(123);
    public static final DbRecord recordAfterOneUpdate = new DbRecord(234);

    public static final ImmutableDbTable EMPTY_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB();
    public static final ImmutableDbTable ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB(Map.of(1, recordForOneInsert));
    public static final ImmutableDbTable ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB(Map.of(1, recordAfterOneUpdate));

    public static final ImmutableDbTable TWO_RECORDS_AFTER_TWO_INSERTS_IMMUTABLE_DB_TABLE = ImmutableDbTable.getImmutableTableInDB(Map.of(1, recordForOneInsert, 2, recordAfterOneUpdate));
}
