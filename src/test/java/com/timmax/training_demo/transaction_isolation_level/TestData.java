package com.timmax.training_demo.transaction_isolation_level;

public class TestData {
    public static final SomeRecordInDB recordForOneInsert = new SomeRecordInDB(123);

    public static final SomeTableInDB emptyTable = new SomeTableInDB();

    public static final SomeTableInDB oneRecordTable;
    static {
        oneRecordTable = new SomeTableInDB();
        oneRecordTable.insert(recordForOneInsert);
    }
}
