package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbRec;
import com.timmax.training_demo.transaction_isolation_level.v02.RowId;

import java.util.Objects;

public final class DMLCommandLogElement {
    private final RowId rowId;
    private final DbRec oldDbRec;

    public DMLCommandLogElement(RowId rowId, DbRec oldDbRec) {
        this.rowId = rowId;
        this.oldDbRec = oldDbRec;
    }

    public RowId getRowId() {
        return rowId;
    }

    public DbRec getOldDbRec() {
        return oldDbRec;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DMLCommandLogElement that)) return false;
        return Objects.equals(rowId, that.rowId) && Objects.equals(oldDbRec, that.oldDbRec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowId, oldDbRec);
    }

    @Override
    public String toString() {
        return "DMLCommandLogElement{" +
                "rowId=" + rowId +
                ", oldDbRec=" + oldDbRec +
                '}';
    }
}
