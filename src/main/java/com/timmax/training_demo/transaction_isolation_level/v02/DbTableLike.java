package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.*;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    public static final String ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE = "ERROR: Duplicate key value violates unique constraint (combinations of all fields must be unique).";

    protected final DbFields dbFields;
    protected final Map<Integer, DbRec> rowId_DbRec_Map = new HashMap<>();
    protected Integer lastInsertedRowId = 0;

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
        for (DbRec dbRec : rowId_DbRec_Map.values()) {
            if (whereFunc == null || whereFunc.where(dbRec)) {
                dbSelect.insert0(dbRec);
            }
        }
        return dbSelect;
    }

    protected void insert0(DbRec newDbRec) {
        lastInsertedRowId++;
        if (rowId_DbRec_Map.put(lastInsertedRowId, new DbRec(newDbRec)) != null) {
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
    }

    protected void insert0(List<DbRec> newDbRecList) {
        newDbRecList.forEach(this::insert0);
    }

    @Override
    public String toString() {
        return "DbTableLike{" +
                "dbFields=" + dbFields +
                ", rowId_DbRec_Map=" + rowId_DbRec_Map +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbTableLike that)) return false;
        return Objects.equals(dbFields, that.dbFields) && Objects.equals(rowId_DbRec_Map, that.rowId_DbRec_Map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbFields, rowId_DbRec_Map);
    }
}
