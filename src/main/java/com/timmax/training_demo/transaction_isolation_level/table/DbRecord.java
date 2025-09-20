package com.timmax.training_demo.transaction_isolation_level.table;

import java.util.Objects;

public class DbRecord {
    private final int field1;

    public DbRecord(int field1) {
        this.field1 = field1;
    }

    public int getField1() {
        return field1;
    }

    @Override
    public String toString() {
        return "DbRecord{" +
                "field1=" + field1 +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DbRecord that = (DbRecord) o;
        return field1 == that.field1;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field1);
    }
}
