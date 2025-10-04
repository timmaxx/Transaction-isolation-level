package com.timmax.training_demo.transaction_isolation_level.v02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public non-sealed class DbTab extends DbTableLike {
    protected static final Logger logger = LoggerFactory.getLogger(DbTab.class);

    private static final String TABLE_IS_RO = "The table '%s' is read only.";
    private static final String YOU_CANNOT_INSERT = "You cannot insert any row into this table.";
    private static final String YOU_CANNOT_UPDATE = "You cannot update any row in this table.";
    private static final String YOU_CANNOT_DELETE = "You cannot delete any row from this table.";

    private final DbTabName dbTabName;
    private boolean readOnly;

    public DbTab(DbTabName dbTabName, DbFields dbFields, boolean readOnly) {
        super(dbFields);
        this.dbTabName = dbTabName;
        this.readOnly = readOnly;
    }

    public DbTab(DbTab dbTab, boolean readOnly) {
        this(dbTab.dbTabName, dbTab.dbFields, readOnly);

/*      //  Рабочий вариант
        for(DbRec dbRec : dbTab.dbRecs) {
            this.dbRecs.add(new DbRec(dbRec));
        }
*/
        //  Плохой вариант
        this.dbRecs.addAll(dbTab.dbRecs);
    }

    public DbTab(DbTab dbTab, boolean readOnly, Set<DbRec> dbRecs) {
        this(dbTab, readOnly);

/*      //  Рабочий вариант
        for (DbRec dbRec : dbRecs) {
            this.dbRecs.add(new DbRec(dbRec));
        }
*/

        //  Плохой вариант
        this.dbRecs.addAll(dbRecs);
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
        return super.toString() +
                "DbTab{" +
                "dbTabName='" + dbTabName + '\'' +
////                 ", readOnly=" + readOnly +
//                ", dbFields=" + dbFields +
//                ", dbRecs=" + dbRecs +
                '}';
    }
}
