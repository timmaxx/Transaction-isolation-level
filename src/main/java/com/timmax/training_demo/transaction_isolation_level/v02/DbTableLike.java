package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    protected final DbFields dbFields;
    protected final Set<DbRec> dbRecs = new HashSet<>();

    public DbTableLike(DbFields dbFields) {
        this.dbFields = dbFields;
    }

    public DbSelect select() {
        return select0(null);
    }

    public DbSelect select(WhereFunc whereFunc) {
        return select0(whereFunc);
    }

    private DbSelect select0(WhereFunc whereFunc) {
        DbSelect dbSelect = new DbSelect(this.dbFields);
        for (DbRec dbRec : dbRecs) {
            if (whereFunc == null || whereFunc.where(dbRec)) {
                dbSelect.insert0(dbRec);
            }
        }
        return dbSelect;
    }

    protected void insert0(DbRec dbRec) {
        dbRecs.add(new DbRec(dbRec));
    }

    @Override
    public String toString() {
        return super.toString() + " " +
                System.identityHashCode(this) + " " +
                "DbTableLike{" +
                "dbFields=" + dbFields +
                ", dbRecs=" + dbRecs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbTableLike that)) return false;
        return Objects.equals(dbFields, that.dbFields) && Objects.equals(dbRecs, that.dbRecs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbFields, dbRecs);
    }
}
