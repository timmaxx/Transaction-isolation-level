package com.timmax.training_demo.transaction_isolation_level.table;

public record DbRecord(int field1) {

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

}
