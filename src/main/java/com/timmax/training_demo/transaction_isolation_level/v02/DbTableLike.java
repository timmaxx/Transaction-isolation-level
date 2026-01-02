package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.*;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.ResultOfDQLCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.INSERT;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    protected static final Logger logger = LoggerFactory.getLogger(DbTableLike.class);

    public static final String ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE = "ERROR: Duplicate key value violates unique constraint (combinations of all fields must be unique).";

    private static final String ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL = "ERROR: Inner trouble. You cannot set WhereFunc into null!";

    protected final DbFields dbFields;
    protected final Map<Integer, DbRec> rowId_DbRec_Map = new HashMap<>();

    private Integer lastInsertedRowId = 0;

    public DbTableLike(DbFields dbFields) {
        this.dbFields = dbFields;
    }

    protected void validateIsWhereFuncNull(WhereFunc whereFunc) {
        if (whereFunc == null) {
            logger.error(ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
            throw new NullPointerException(ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        }
    }

    public ResultOfDQLCommand select() {
        return select(dbRec -> true);
    }

    public ResultOfDQLCommand select(WhereFunc whereFunc) {
        validateIsWhereFuncNull(whereFunc);
        return select0(whereFunc);
    }

    private ResultOfDQLCommand select0(WhereFunc whereFunc) {
        DbSelect dbSelect = new DbSelect(this.dbFields);
        for (DbRec dbRec : rowId_DbRec_Map.values()) {
            if (whereFunc.where(dbRec)) {
                dbSelect.insert0(dbRec);
            }
        }
        return new ResultOfDQLCommand(dbSelect);
    }

    //  ToDo:   Нужно выделить общий функционал с
    //          private ResultOfDMLCommand update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc)
    //          в пунктах 3 и 3.2 и привести к единообразию и убрать дублирующийся код
    //          (для всех методов insert0, insert00, insert000)
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

        insert000(rowId, newDbRec);

        //  ToDo:   Здесь указываю null, но нужно сделать (иерархию классов) так чтобы null не указывать.
        dmlCommandLog.push(new DMLCommandLogElement(rowId, null));
    }

    protected void insert000(Integer rowId, DbRec newDbRec) {
        if (rowId_DbRec_Map.put(rowId, new DbRec(newDbRec)) != null) {
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
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
