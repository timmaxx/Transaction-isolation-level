package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public non-sealed class DbTab extends DbTableLike {
    protected static final Logger logger = LoggerFactory.getLogger(DbTab.class);

    static final String TABLE_IS_RO = "The table '%s' is read only.";
    static final String YOU_CANNOT_INSERT = "You cannot insert any row into this table.";
    static final String YOU_CANNOT_UPDATE = "You cannot update any row in this table.";
    private static final String YOU_CANNOT_DELETE = "You cannot delete any row from this table.";

    private final DbTabName dbTabName;
    private final boolean readOnly;

    public DbTab(DbTabName dbTabName, DbFields dbFields, boolean readOnly) {
        super(dbFields);
        this.dbTabName = dbTabName;
        this.readOnly = readOnly;
    }

    public DbTab(DbTab dbTab, boolean readOnly) {
        this(dbTab.dbTabName, dbTab.dbFields, readOnly);
        this.dbRecs.addAll(dbTab.dbRecs.stream().map(dbRec0 -> new DbRec0(dbRec0.getSuper(), this)).toList());
    }

    public DbTab(DbTab dbTab, boolean readOnly, Set<DbRec> dbRecs) {
        this(dbTab, readOnly);
        this.dbRecs.addAll(dbRecs.stream().map(dbRec -> new DbRec0(dbRec, this)).toList());
    }

    private void validateReadOnlyTable(String msgInsUpdDel) {
        if (!readOnly) {
            return;
        }
        throw new DbDataAccessException(String.format(TABLE_IS_RO, dbTabName) + " " + msgInsUpdDel);
    }

    public void insert(DbRec dbRec) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        insert0(dbRec);
    }

    public void delete() {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        delete0();
    }

    public void delete(WhereFunc whereFunc) {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        delete0(whereFunc);
    }

    private void delete0() {
        dbRecs.clear();
    }

    private void delete0(WhereFunc whereFunc) {
        dbRecs.removeIf(whereFunc::where);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc) {
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        update0(updateSetCalcFunc, null);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        update0(updateSetCalcFunc, whereFunc);
    }

    private void update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        for (DbRec dbRec : dbRecs) {
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
