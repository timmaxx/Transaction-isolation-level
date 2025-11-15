package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbRec;
import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;

import java.util.Objects;

public final class DMLCommandQueueLogElement {
    private final DMLCommandQueueLogElementType dmlCommandQueueLogElementType;
    private final DbTab dbTab;
    private final Integer rowId;
    private final DbRec dbRec;

    public DMLCommandQueueLogElement(
            DMLCommandQueueLogElementType dmlCommandQueueLogElementType,
            DbTab dbTab,
            Integer rowId,
            DbRec dbRec
    ) {
        this.dmlCommandQueueLogElementType = dmlCommandQueueLogElementType;
        this.dbTab = dbTab;
        this.rowId = rowId;
        this.dbRec = dbRec;
    }

    public DMLCommandQueueLogElementType getDmlCommandqueuelogelementtype() {
        return dmlCommandQueueLogElementType;
    }

    public DbTab getDbTab() {
        return dbTab;
    }

    public Integer getRowId() {
        return rowId;
    }

    public DbRec getDbRec() {
        return dbRec;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DMLCommandQueueLogElement) obj;
        return Objects.equals(this.dmlCommandQueueLogElementType, that.dmlCommandQueueLogElementType) &&
                Objects.equals(this.dbTab, that.dbTab) &&
                Objects.equals(this.rowId, that.rowId) &&
                Objects.equals(this.dbRec, that.dbRec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dmlCommandQueueLogElementType, dbTab, rowId, dbRec);
    }

    @Override
    public String toString() {
        return "DMLCommandQueueLogElement[" +
                "DMLCommandQueueLogElementType=" + dmlCommandQueueLogElementType + ", " +
                "dbTab=" + dbTab + ", " +
                "rowId=" + rowId + ", " +
                "dbRec=" + dbRec + ']';
    }

}
