package com.timmax.training_demo.transaction_isolation_level;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SomeRecordInDB that = (SomeRecordInDB) o;
        return field1 == that.field1;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field1);
    }
}
