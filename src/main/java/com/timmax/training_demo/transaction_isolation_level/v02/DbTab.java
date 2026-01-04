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


    //  Публичный INSERT одной записи
    public ResultOfDMLCommand insert(DbRec newDbRec) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        return insert0(newDbRec);
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
        rowId_DbRec_Map.keySet().removeIf(key -> key.equals(rowId));
    }

    //  ToDo:   Не должен быть public!
    @Override
    public void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        if (rowId_DbRec_Map.put(rowId, new DbRec(oldDbRec)) != null) {
            //  Условие в принципе не должно никогда срабатывать, т.к. происходит rollback!!!
            //  ToDo:   Ошибка должна быть более суровой и дополнена текстом.
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
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
        validateIsUpdateSetCalcFuncNull(updateSetCalcFunc);
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        return update0(updateSetCalcFunc, whereFunc);
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

    //  Вычисление новых записей (только для UPDATE);
    //  Логирование записей, подлежащих удалению или обновлению;
    //  Удаление этих записей.
    private void delete00ForDeletingAndUpdating(WhereFunc whereFunc, Map<Integer, DbRec> new_rowId_DbRec_Map, DMLCommandLog dmlCommandLog, UpdateSetCalcFunc updateSetCalcFunc) {
        //  Количество записей, до выполнения
        int countBeforeAll = rowId_DbRec_Map.size();

        //  1.  Подготовка для тех записей, которые попали в where:
        //  1.1.    вычисляются новые значения (только для UPDATE),
        //  1.2.    пишутся в лог отката
        for (Map.Entry<Integer, DbRec> entry: rowId_DbRec_Map.entrySet()) {
            DbRec oldDbRec = entry.getValue();
            if (whereFunc.where(oldDbRec)) {
                Integer rowId = entry.getKey();
                DbRec newDbRec;
                if (updateSetCalcFunc == null) {
                    //  Код для DELETE:
                    newDbRec = null;
                } else {
                    //  Код для UPDATE:
                    //  Берём все поля из старой записи и переписываем те, которые поступили ч/з функцию setCalcFunc.
                    newDbRec = new DbRec(oldDbRec, updateSetCalcFunc.setCalcFunc(oldDbRec));
                }
                //  Новую запись записываем в промежуточную мапу
                new_rowId_DbRec_Map.put(rowId, newDbRec);

                //  Создаём запись в журнале отката
                dmlCommandLog.push(new DMLCommandLogElement(rowId, oldDbRec));
            }
        }

        //  Количество записей, которые должны быть удалены
        int countForProcessing = new_rowId_DbRec_Map.size();

        //  2.  Удаление записей, удовлетворяющих where
        rowId_DbRec_Map.keySet().removeAll(new_rowId_DbRec_Map.keySet());

        //  2.2.    Проверка удаления по количеству записей
        //  Количество всех записей в основной таблице, которые получились после промежуточного удаления
        int countAfterRemoving = rowId_DbRec_Map.size();

        if (countBeforeAll - countForProcessing != countAfterRemoving) {
            logger.error("countBeforeAll({}) - countForProcessing({}) != countAfterRemoving({})", countBeforeAll, countForProcessing, countAfterRemoving);
            logger.error("new_rowId_DbRec_Map = {}", new_rowId_DbRec_Map);
            logger.error("after remove:  rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("beforeCount - countForProcessing != countAfterRemoving");
        }
    }
}
