package com.timmax.training_demo.transaction_isolation_level.v02;

public non-sealed class DbTab extends DbTableLike {
    private static final String TABLE_IS_RO = "The table '%s' is read only.";
    private static final String YOU_CANNOT_INSERT = "You cannot insert any row into this table.";
    private static final String YOU_CANNOT_UPDATE = "You cannot update any row in this table.";
    private static final String YOU_CANNOT_DELETE = "You cannot delete any row from this table.";

    private final DbTabName dbTabName;
    private boolean readOnly = false;

    public DbTab(DbTabName dbTabName, DbFields dbFields) {
        super(dbFields);
        this.dbTabName = dbTabName;
    }

    public void setReadOnly() {
        readOnly = true;
    }

    private void validateReadOnlyTable(String msgInsUpdDel) throws DataAccessException {
        if (!readOnly) {
            return;
        }
        String reason = String.format(TABLE_IS_RO, dbTabName) + " " + msgInsUpdDel;
        throw new DataAccessException(reason);
    }

    public void insert(DbRec dbRec) throws DataAccessException {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        insert0(dbRec);
    }

    public void delete() throws DataAccessException {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        delete0();
    }

    public void delete(WhereFunc whereFunc) throws DataAccessException {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        delete0(whereFunc);
    }

    private void delete0() {
        dbRecs.clear();
    }

    private void delete0(WhereFunc whereFunc) {
        dbRecs.removeIf(whereFunc::where);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc) throws DataAccessException {
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        update0(updateSetCalcFunc, null);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) throws DataAccessException {
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        update0(updateSetCalcFunc, whereFunc);
    }

    private void update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        for(DbRec dbRec : dbRecs) {
            if (whereFunc == null || whereFunc.where(dbRec)) {
                dbRec.setAll(updateSetCalcFunc.setCalcFunc(dbRec));
            }
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
