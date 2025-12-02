package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import java.util.Objects;

public final class DMLCommandLogElement {
    private final Integer rowId;

    public DMLCommandLogElement(Integer rowId) {
        this.rowId = rowId;
    }

    public Integer getRowId() {
        return rowId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DMLCommandLogElement that)) return false;
        return Objects.equals(rowId, that.rowId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rowId);
    }

    @Override
    public String toString() {
        return "DMLCommandLogElement{" +
                "rowId=" + rowId +
                '}';
    }
}

/*
public final class DMLCommandLogElement {
    private final Integer rowId;
    private final DbRec dbRec;

    public DMLCommandLogElement(Integer rowId, DbRec dbRec) {
        this.rowId = rowId;
        this.dbRec = dbRec;
    }

    public Integer getRowId() {
        return rowId;
    }

    public DbRec getDbRec() {
        return dbRec;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DMLCommandLogElement that)) return false;
        return Objects.equals(rowId, that.rowId) && Objects.equals(dbRec, that.dbRec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowId, dbRec);
    }

    @Override
    public String toString() {
        return "DMLCommandLogElement{" +
                "rowId=" + rowId +
                ", dbRec=" + dbRec +
                '}';
    }
}
*/
