package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.*;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.ResultOfDQLCommand;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.INSERT;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    public static final String ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE = "ERROR: Duplicate key value violates unique constraint (combinations of all fields must be unique).";

    protected final DbFields dbFields;
    protected final Map<Integer, DbRec> rowId_DbRec_Map = new HashMap<>();

    private Integer lastInsertedRowId = 0;

    public DbTableLike(DbFields dbFields) {
        this.dbFields = dbFields;
    }

    public ResultOfDQLCommand select() {
        return select(null);
    }

    public ResultOfDQLCommand select(WhereFunc whereFunc) {
        return select0(whereFunc);
    }

    private ResultOfDQLCommand select0(WhereFunc whereFunc) {
        DbSelect dbSelect = new DbSelect(this.dbFields);
        for (DbRec dbRec : rowId_DbRec_Map.values()) {
            if (whereFunc == null || whereFunc.where(dbRec)) {
                dbSelect.insert0(dbRec);
            }
        }
        return new ResultOfDQLCommand(dbSelect);
    }

    protected ResultOfDMLCommand insert0(DbRec newDbRec) {
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, INSERT);

        insert00(dmlCommandLog, newDbRec);

        return new ResultOfDMLCommand(dmlCommandLog);
    }

    protected ResultOfDMLCommand insert0(List<DbRec> newDbRec_List) {
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, INSERT);

        for (DbRec newDbRec : newDbRec_List) {
            insert00(dmlCommandLog, newDbRec);
        }

        return new ResultOfDMLCommand(dmlCommandLog);
    }

    private void insert00(DMLCommandLog dmlCommandLog, DbRec newDbRec) {
        Integer rowId;
        rowId = ++lastInsertedRowId;
        insert00(dmlCommandLog, rowId, newDbRec);
    }

    private void insert00(DMLCommandLog dmlCommandLog, Integer rowId, DbRec newDbRec) {
        if (rowId_DbRec_Map.put(rowId, new DbRec(newDbRec)) != null) {
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
        //  ToDo:   Здесь указываю null, но нужно сделать (иерархию классов) так чтобы null не указывать.

        dmlCommandLog.push(new DMLCommandLogElement(rowId, null));
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

    public abstract void rollbackOfInsert(Integer rowId);

    public abstract void rollbackOfDelete(Integer rowId, DbRec oldDbRec);

    public abstract void rollbackOfUpdate(Integer rowId, DbRec oldDbRec);
}
