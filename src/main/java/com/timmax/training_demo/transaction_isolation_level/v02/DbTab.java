package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLog;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElement;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.ResultOfDMLCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.DELETE;
import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.UPDATE;

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

    public ResultOfDMLCommand insert(List<DbRec> newDbRec_List) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        return insert0(newDbRec_List);
    }

    public ResultOfDMLCommand delete() {
        return delete(null);
    }

    public ResultOfDMLCommand delete(WhereFunc whereFunc) {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        return delete0(whereFunc);
    }

    //  ToDo:   Удалить после рефакторинга
    //          private ResultOfDMLCommand delete0(WhereFunc whereFunc)
    private ResultOfDMLCommand delete0() {

        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, DELETE);
        for (Map.Entry<Integer, DbRec> entry: rowId_DbRec_Map.entrySet()) {
            Integer rowId = entry.getKey();
            DbRec oldDbRec = entry.getValue();
            dmlCommandLog.push(new DMLCommandLogElement(rowId, oldDbRec));
        }

        rowId_DbRec_Map.clear();

        return new ResultOfDMLCommand(dmlCommandLog);
    }

    private ResultOfDMLCommand delete0(WhereFunc whereFunc) {
        //  ToDo:   Вместо вызова delete0() при whereFunc == null можно было-бы сделать так,
        //          чтобы whereFunc всегда давала true.
        //          Тогда можно было-бы удалить delete0().
        if (whereFunc == null) {
            return delete0();
        }

        //  ToDo:   Вероятно можно оптимизировать, т.к. мапа обходится дважды:
        //          - сначала логирование для возможности отката,
        //          - потом само удаление.
        //
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, DELETE);
        for (Map.Entry<Integer, DbRec> entry: rowId_DbRec_Map.entrySet()) {
            if (whereFunc.where(entry.getValue())) {
                Integer rowId = entry.getKey();
                DbRec oldDbRec = entry.getValue();
                dmlCommandLog.push(new DMLCommandLogElement(rowId, oldDbRec));
            }
        }

        rowId_DbRec_Map.values().removeIf(whereFunc::where);
        return new ResultOfDMLCommand(dmlCommandLog);
    }

    //  ToDo:   Не должен быть public!
    @Override
    public void rollbackOfInsert(Integer rowId) {
        //  Здесь делается удаление одной строки, но при этом не пишется лог для rollback
        rowId_DbRec_Map.keySet().removeIf(key -> key.equals(rowId));
    }

    @Override
    public void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        if (rowId_DbRec_Map.put(rowId, new DbRec(oldDbRec)) != null) {
            //  Условие в принципе не должно никогда срабатывать, т.к. происходит rollback!!!
            //  ToDo:   Ошибка должна быть более суровой и дополнена текстом.
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
    }

    @Override
    public void rollbackOfUpdate(Integer rowId, DbRec oldDbRec) {
        rollbackOfInsert(rowId);
        rollbackOfDelete(rowId, oldDbRec);
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
        Map<Integer, DbRec> old_rowId_DbRec_Map = new HashMap<>();
        Map<Integer, DbRec> new_rowId_DbRec_Map = new HashMap<>();
        int beforeCount = rowId_DbRec_Map.size();

        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, UPDATE);
        for (Map.Entry<Integer, DbRec> entry : rowId_DbRec_Map.entrySet()) {
            if (whereFunc == null || whereFunc.where(entry.getValue())) {
                Integer rowId = entry.getKey();
                DbRec oldDbRec =  entry.getValue();
                old_rowId_DbRec_Map.put(rowId, oldDbRec);
                //  Берём все поля из старой записи и переписываем те, которые поступили ч/з функцию setCalcFunc.
                DbRec newDbRec = new DbRec(oldDbRec, updateSetCalcFunc.setCalcFunc(oldDbRec));
                new_rowId_DbRec_Map.put(rowId, newDbRec);
                dmlCommandLog.push(new DMLCommandLogElement(rowId, oldDbRec));
            }
        }

        int oldCount = old_rowId_DbRec_Map.size();
        int newCount = new_rowId_DbRec_Map.size();

        if (oldCount != newCount) {
            logger.error("oldCount != newCount");
            logger.error("beforeCount = {}, oldCount = {}, newCount = {}", beforeCount, oldCount, newCount);
            throw new RuntimeException("oldCount != newCount");
        }

        rowId_DbRec_Map.keySet().removeAll(old_rowId_DbRec_Map.keySet());
        int afterRemoveCount = rowId_DbRec_Map.size();

        if (beforeCount - oldCount != afterRemoveCount) {
            logger.error("beforeCount - oldCount != afterRemoveCount");
            logger.error("old_rowId_DbRec_Map = {}", old_rowId_DbRec_Map);
            logger.error("new_rowId_DbRec_Map = {}", new_rowId_DbRec_Map);
            logger.error("beforeCount = {}, oldCount = {}, newCount = {}", beforeCount, oldCount, newCount);
            logger.error("afterRemoveCount = {}", afterRemoveCount);
            logger.error("after remove:  rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("beforeCount - oldCount != afterRemoveCount");
        }

        insert0(new_rowId_DbRec_Map);
        int afterCount = rowId_DbRec_Map.size();

        if (afterRemoveCount + newCount != afterCount) {
            logger.error("afterRemoveCount + newCount != afterCount");
            logger.error("afterRemoveCount = {}, newCount = {}, afterCount = {}", afterRemoveCount, newCount, afterCount);
            logger.error("after insert0: rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("afterRemoveCount + newCount != afterCount");
        }

        return new ResultOfDMLCommand(dmlCommandLog);
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
