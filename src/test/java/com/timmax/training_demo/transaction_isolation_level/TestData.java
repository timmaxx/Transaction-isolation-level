package com.timmax.training_demo.transaction_isolation_level;

public class TestData {
    public static final SomeRecordInDB recordForOneInsert = new SomeRecordInDB(123);

    public static final SomeTableInDB afterOneRecordWasInserted = new SomeTableInDB();
    static {
        afterOneRecordWasInserted.insert(recordForOneInsert);
    }
}
