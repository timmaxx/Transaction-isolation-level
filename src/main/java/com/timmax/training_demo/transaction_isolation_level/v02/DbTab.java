package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLog;
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
        insert0(dbTab.getRows().stream().toList());
    }

    public DbTab(DbTab dbTab, boolean readOnly, List<DbRec> dbRec_List) {
        this(dbTab, readOnly);
        insert0(dbRec_List);
    }


    //  Публичный INSERT одной записи
    public ResultOfDMLCommand insert(DbRec newDbRec) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        return insert0(List.of(newDbRec));
    }

    //  Публичный INSERT списка записей
    public ResultOfDMLCommand insert(List<DbRec> newDbRec_List) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        return insert0(newDbRec_List);
    }

    //  Публичный DELETE всех записей (без WHERE)
    public ResultOfDMLCommand delete() {
        return delete(dbRec -> true);
    }

    //  Публичный DELETE выборочных записей (с WHERE)
    public ResultOfDMLCommand delete(WhereFunc whereFunc) {
        Objects.requireNonNull(whereFunc, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        return delete0(whereFunc);
    }

    //  ToDo:   Не должен быть public!
    @Override
    public void rollbackOfInsert(Integer rowId) {
        //  Здесь делается удаление одной строки, но при этом не пишется лог для rollback
        delete000(Set.of(rowId));
    }

    //  ToDo:   Не должен быть public!
    @Override
    public void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        //  Здесь делается вставка одной строки, но при этом не пишется лог для rollback
        insert00(Map.of(rowId, oldDbRec));
    }

    //  ToDo:   Не должен быть public!
    @Override
    public void rollbackOfUpdate(Integer rowId, DbRec oldDbRec) {
        rollbackOfInsert(rowId);
        rollbackOfDelete(rowId, oldDbRec);
    }

    //  Публичный UPDATE всех записей (без WHERE)
    public ResultOfDMLCommand update(UpdateSetCalcFunc updateSetCalcFunc) {
        return update(updateSetCalcFunc, dbRec -> true);
    }

    //  Публичный UPDATE выборочных записей (с WHERE)
    public ResultOfDMLCommand update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Objects.requireNonNull(whereFunc, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        Objects.requireNonNull(updateSetCalcFunc, ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL);
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        return update0(updateSetCalcFunc, whereFunc);
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "dbTabName='" + dbTabName + '\'' +
                ", readOnly=" + readOnly +
                ", dbFields=" + dbFields +
                '}';
    }


    private void validateReadOnlyTable(String msgInsUpdDel) {
        if (readOnly) {
            throw new DbDataAccessException(String.format(ERROR_TABLE_IS_RO, dbTabName) + " " + msgInsUpdDel);
        }
    }

    private ResultOfDMLCommand delete0(WhereFunc whereFunc) {
        Map<Integer, DbRec> new_rowId_DbRec_Map = new HashMap<>();
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, DELETE);

        //  Для DELETE updateSetCalcFunc делаем null
        delete00ForDeletingAndUpdating(whereFunc, new_rowId_DbRec_Map, dmlCommandLog, null);

        //  Здесь нет кода, который есть в update (т.е. вставки)

        return new ResultOfDMLCommand(dmlCommandLog);
    }

    private ResultOfDMLCommand update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Map<Integer, DbRec> new_rowId_DbRec_Map = new HashMap<>();
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, UPDATE);

        //  Для UPDATE updateSetCalcFunc передаём в метод
        delete00ForDeletingAndUpdating(whereFunc, new_rowId_DbRec_Map, dmlCommandLog, updateSetCalcFunc);

        //  Наличием этого кода отличается от
        //  private ResultOfDMLCommand delete0(WhereFunc whereFunc)
        //  Вставляем в основную таблицу те записи, которые были созданы выше в new_rowId_DbRec_Map
        insert00(new_rowId_DbRec_Map);

        return new ResultOfDMLCommand(dmlCommandLog);
    }
}
