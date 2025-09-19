package com.timmax.training_demo.transaction_isolation_level;

public class SomeRecordInDB {
    private final int field1;

    public SomeRecordInDB(int field1) {
        this.field1 = field1;
    }

    public int getField1() {
        return field1;
    }

    @Override
    public String toString() {
        return "SomeRecordInDB{" +
                "field1=" + field1 +
                '}';
    }
}
