package com.timmax.training_demo.transaction_isolation_level.v02;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    protected final DbFields dbFields;
    protected final Set<DbRec0> dbRecs = new HashSet<>();

    public DbTableLike(DbFields dbFields) {
        this.dbFields = dbFields;
    }

    public DbSelect select() throws SQLException {
        return select0(null);
    }

    public DbSelect select(WhereFunc whereFunc) throws SQLException {
        return select0(whereFunc);
    }

    private DbSelect select0(WhereFunc whereFunc) throws SQLException {
        DbSelect dbSelect = new DbSelect(this.dbFields);
        for (DbRec dbRec : dbRecs) {
            if (whereFunc == null || whereFunc.where(dbRec)) {
                dbSelect.insert0(dbRec);
            }
        }
        return dbSelect;
    }

    protected void insert0(DbRec dbRec) throws SQLException {
        dbRec.verify(dbFields);
        dbRecs.add(new DbRec0(dbRec, this));
    }

    @Override
    public String toString() {
        return "DbTableLike{" +
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
