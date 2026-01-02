package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLog;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElement;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.ResultOfDMLCommand;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.DELETE;
import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.UPDATE;

public non-sealed class DbTab extends DbTableLike {
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
        return delete(dbRec -> true);
    }

    public ResultOfDMLCommand delete(WhereFunc whereFunc) {
        validateIsWhereFuncNull(whereFunc);
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        return delete0(whereFunc);
    }

    private ResultOfDMLCommand delete0(WhereFunc whereFunc) {
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
        return update(updateSetCalcFunc, dbRec -> true);
    }

    public ResultOfDMLCommand update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        validateIsWhereFuncNull(whereFunc);
        validateIsUpdateSetCalcFuncNull(updateSetCalcFunc);
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        return update0(updateSetCalcFunc, whereFunc);
    }

    private ResultOfDMLCommand update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Map<Integer, DbRec> new_rowId_DbRec_Map = new HashMap<>();

        //  Общее количество всех записей в основной таблице
        int countBeforeAll = rowId_DbRec_Map.size();

        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, UPDATE);

        //  Наполняем new_rowId_DbRec_Map записями с новыми значениями
        for (Map.Entry<Integer, DbRec> entry : rowId_DbRec_Map.entrySet()) {
            DbRec oldDbRec =  entry.getValue();
            if (whereFunc.where(oldDbRec)) {
                Integer rowId = entry.getKey();
                //  Берём все поля из старой записи и переписываем те, которые поступили ч/з функцию setCalcFunc.
                DbRec newDbRec = new DbRec(oldDbRec, updateSetCalcFunc.setCalcFunc(oldDbRec));
                new_rowId_DbRec_Map.put(rowId, newDbRec);
                //  Создаём запись в журнале отката
                dmlCommandLog.push(new DMLCommandLogElement(rowId, oldDbRec));
            }
        }

        //  Количество записей, которые должны быть обновлены
        int countForUpdating = new_rowId_DbRec_Map.size();

        //  Удаляем все записи, которые нужно будет обновить
        rowId_DbRec_Map.keySet().removeAll(new_rowId_DbRec_Map.keySet());

        //  Количество всех записей в основной таблице, которые получились после промежуточного удаления
        int countAfterRemoving = rowId_DbRec_Map.size();

        if (countBeforeAll - countForUpdating != countAfterRemoving) {
            logger.error("countBeforeAll({}) - countForUpdating({}) != countAfterRemoving({})", countBeforeAll, countForUpdating, countAfterRemoving);
            logger.error("new_rowId_DbRec_Map = {}", new_rowId_DbRec_Map);
            logger.error("after remove:  rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("beforeCount - countForUpdating != countAfterRemoving");
        }

        //  Вставляем в основную таблицу те записи, которые были созданы выше в new_rowId_DbRec_Map
        for (Map.Entry<Integer, DbRec> entry : new_rowId_DbRec_Map.entrySet()) {
            Integer rowId = entry.getKey();
            DbRec newDbRec = entry.getValue();
            //  После этой вставки нет вставки в журнал отката
            insert000(rowId, newDbRec);
        }

        int countAfterAll = rowId_DbRec_Map.size();

        if (countAfterRemoving + countForUpdating != countAfterAll) {
            logger.error("countAfterRemoving({}) + countForUpdating({}) != countAfterAll({})", countAfterRemoving, countForUpdating, countAfterAll);
            logger.error("after insert0: rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("countAfterRemoving + countForUpdating != countAfterAll");
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
