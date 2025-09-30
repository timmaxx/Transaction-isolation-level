package com.timmax.training_demo.transaction_isolation_level.v02;

public non-sealed class DbTab extends DbTableLike {
    private final DbTabName dbTabName;
    private boolean readOnly = false;

    public DbTab(DbTabName dbTabName, DbFields dbFields) {
        super(dbFields);
        this.dbTabName = dbTabName;
    }

    public void setReadOnly() {
        readOnly = true;
    }

    public void insert(DbRec dbRec) {
        if (readOnly) {
            throw new RuntimeException("The table '" + dbTabName + "' is read only. You cannot insert any row into this table.");
        }
        insert0(dbRec);
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "dbTabName='" + dbTabName + '\'' +
                 ", readOnly=" + readOnly +
                ", dbFields=" + dbFields +
                ", dbRecs=" + dbRecs +
                '}';
    }
}
