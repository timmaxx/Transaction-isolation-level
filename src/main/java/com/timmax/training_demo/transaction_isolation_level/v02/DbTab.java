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

    public void delete() {
        if (readOnly) {
            throw new RuntimeException("The table '" + dbTabName + "' is read only. You cannot delete any row from this table.");
        }
        delete0();
    }

    public void delete(WhereFunc whereFunc) {
        if (readOnly) {
            throw new RuntimeException("The table '" + dbTabName + "' is read only. You cannot delete any row from this table.");
        }
        delete0(whereFunc);
    }

    private void delete0() {
        dbRecs.clear();
    }

    private void delete0(WhereFunc whereFunc) {
        dbRecs.removeIf(whereFunc::where);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc) {
        if (readOnly) {
            throw new RuntimeException("The table '" + dbTabName + "' is read only. You cannot update any row in this table.");
        }
        update0(updateSetCalcFunc);
    }

    private void update0(UpdateSetCalcFunc updateSetCalcFunc) {
        for(DbRec dbRec : dbRecs) {
            dbRec.setAll(updateSetCalcFunc.setCalcFunc(dbRec));
        }
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
