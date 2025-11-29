package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.ResultOfDMLCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public non-sealed class DbTab extends DbTableLike {
    protected static final Logger logger = LoggerFactory.getLogger(DbTab.class);

    private static final String ERROR_TABLE_IS_RO = "ERROR: The table '%s' is read only.";
    private static final String YOU_CANNOT_INSERT = "You cannot insert any row into this table.";
    private static final String YOU_CANNOT_UPDATE = "You cannot update any row in this table.";
    private static final String YOU_CANNOT_DELETE = "You cannot delete any row from this table.";

    static final String ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT = ERROR_TABLE_IS_RO + " " + YOU_CANNOT_INSERT;
    static final String ERROR_TABLE_IS_RO_YOU_CANNOT_UPDATE = ERROR_TABLE_IS_RO + " " + YOU_CANNOT_UPDATE;
    static final String ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE = ERROR_TABLE_IS_RO + " " + YOU_CANNOT_DELETE;

    static final String ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL = "ERROR: updateSetCalcFunc is null, but you cannot make it null!";

    private final DbTabName dbTabName;
    private final boolean readOnly;

    public DbTab(DbTabName dbTabName, DbFields dbFields, boolean readOnly) {
        super(dbFields);
        this.dbTabName = dbTabName;
        this.readOnly = readOnly;
    }

    public DbTab(DbTab dbTab, boolean readOnly) {
        this(dbTab.dbTabName, dbTab.dbFields, readOnly);
        dbTab.rowId_DbRec_Map.values().forEach(this::insert0);
    }

    public DbTab(DbTab dbTab, boolean readOnly, List<DbRec> dbRec_List) {
        this(dbTab, readOnly);
        dbRec_List.forEach(this::insert0);
    }

    private void validateReadOnlyTable(String msgInsUpdDel) {
        if (readOnly) {
            throw new DbDataAccessException(String.format(ERROR_TABLE_IS_RO, dbTabName) + " " + msgInsUpdDel);
        }
    }

    private void validateIsUpdateSetCalcFuncNull(UpdateSetCalcFunc updateSetCalcFunc) {
        if (updateSetCalcFunc == null) {
            throw new NullPointerException(ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL);
        }
    }

    public ResultOfDMLCommand insert(DbRec newDbRec) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        return insert0(newDbRec);
    }

    public ResultOfDMLCommand delete() {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        return delete0();
    }

    public ResultOfDMLCommand delete(WhereFunc whereFunc) {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        return delete0(whereFunc);
    }

    private ResultOfDMLCommand delete0() {
        //  !!!!!
        rowId_DbRec_Map.clear();
        //  ToDo:   code for rollback
        return new ResultOfDMLCommand(null);
    }

    private ResultOfDMLCommand delete0(WhereFunc whereFunc) {
        if (whereFunc == null) {
            return delete0();
        }
        //  !!!!!
        rowId_DbRec_Map.values().removeIf(whereFunc::where);
        //  ToDo:   code for rollback
        // return new ResultOfDMLCommand(new DMLCommandQueueLog());
        return new ResultOfDMLCommand(null);
    }

    public ResultOfDMLCommand update(UpdateSetCalcFunc updateSetCalcFunc) {
        return update(updateSetCalcFunc, null);
    }

    public ResultOfDMLCommand update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        validateIsUpdateSetCalcFuncNull(updateSetCalcFunc);
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        return update0(updateSetCalcFunc, whereFunc);
    }

    private ResultOfDMLCommand update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        List<DbRec> oldDbRecs = new ArrayList<>();
        List<DbRec> newDbRecs = new ArrayList<>();
        int beforeCount = rowId_DbRec_Map.size();

        for (Map.Entry<Integer, DbRec>  entry : rowId_DbRec_Map.entrySet()) {
            if (whereFunc == null || whereFunc.where(entry.getValue())) {
                oldDbRecs.add(entry.getValue());
                //  Берём все поля из старой записи и переписываем те, которые поступили ч/з функцию setCalcFunc.
                DbRec newDbRec = new DbRec(entry.getValue(), updateSetCalcFunc.setCalcFunc(entry.getValue()));
                newDbRecs.add(newDbRec);
            }
        }

        int oldCount = oldDbRecs.size();
        int newCount = newDbRecs.size();

        if (oldCount != newCount) {
            logger.error("oldCount != newCount");
            logger.error("beforeCount = {}, oldCount = {}, newCount = {}", beforeCount, oldCount, newCount);
            throw new RuntimeException("oldCount != newCount");
        }

        rowId_DbRec_Map.values().removeAll(oldDbRecs);
        int afterRemoveCount = rowId_DbRec_Map.size();

        if (beforeCount - oldCount != afterRemoveCount) {
            logger.error("beforeCount - oldCount != afterRemoveCount");
            logger.error("oldTestRecordSet = {}", oldDbRecs);
            logger.error("newTestRecordSet = {}", newDbRecs);
            logger.error("beforeCount = {}, oldCount = {}, newCount = {}", beforeCount, oldCount, newCount);
            logger.error("afterRemoveCount = {}", afterRemoveCount);
            logger.error("after remove:  rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("beforeCount - oldCount != afterRemoveCount");
        }

        insert0(newDbRecs);
        int afterCount = rowId_DbRec_Map.size();

        if (afterRemoveCount + newCount != afterCount) {
            logger.error("afterRemoveCount + newCount != afterCount");
            logger.error("afterRemoveCount = {}, newCount = {}, afterCount = {}", afterRemoveCount, newCount, afterCount);
            logger.error("after insert0: rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("afterRemoveCount + newCount != afterCount");
        }
        //  !!!!!
        return null;
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "dbTabName='" + dbTabName + '\'' +
                ", readOnly=" + readOnly +
                ", dbFields=" + dbFields +
                ", rowId_DbRec_Map=" + rowId_DbRec_Map +
                '}';
    }
}
