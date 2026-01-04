package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.*;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.ResultOfDQLCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLogElementType.INSERT;

public abstract sealed class DbTableLike permits DbTab, DbSelect {
    public static final String ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE = "ERROR: Duplicate key value violates unique constraint (combinations of all fields must be unique).";

    protected static final Logger logger = LoggerFactory.getLogger(DbTableLike.class);
    protected static final String ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL = "ERROR: Inner trouble. You cannot set WhereFunc into null!";


    protected final DbFields dbFields;

    private final Map<Integer, DbRec> rowId_DbRec_Map = new HashMap<>();
    private Integer lastInsertedRowId = 0;


    public DbTableLike(DbFields dbFields) {
        this.dbFields = dbFields;
    }


    public int size() {
        return rowId_DbRec_Map.size();
    }

    //  Публичный SELECT всех записей (без WHERE)
    public ResultOfDQLCommand select() {
        return select(dbRec -> true);
    }

    //  Публичный SELECT выборочных записей (с WHERE)
    public ResultOfDQLCommand select(WhereFunc whereFunc) {
        Objects.requireNonNull(whereFunc, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        return select0(whereFunc);
    }

    //  В этом классе нет публичного INSERT, но он (INSERT, только protected) понадобится для создания SELECT.
    //  Кроме того, в этом классе тем более нет UPDATE и DELETE (никаких - ни публичных, ни приватных),
    //  т.к. они будут реализовываться только для таблиц.

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

    //  ToDo:   Сделать private
    protected void delete000(Set<Integer> rowIdSet) {
        int countBeforeAll = size();
        int countForProcessing = rowIdSet.size();

        rowId_DbRec_Map.keySet().removeAll(rowIdSet);

        //  Проверка удаления по количеству записей
        //  Количество всех записей в основной таблице, которые получились после промежуточного удаления
        int countAfterRemoving = size();

        if (countBeforeAll - countForProcessing != countAfterRemoving) {
            logger.error("countBeforeAll({}) - countForProcessing({}) != countAfterRemoving({})", countBeforeAll, countForProcessing, countAfterRemoving);
            throw new RuntimeException("beforeCount - countForProcessing != countAfterRemoving");
        }
    }

    //  INSERT списка записей (без ROWID)
    protected ResultOfDMLCommand insert0(List<DbRec> newDbRec_List) {
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, INSERT);

        insert00(dmlCommandLog, newDbRec_List);

        return new ResultOfDMLCommand(dmlCommandLog);
    }

    //  Вставка группы записей с заранее определёнными ROWID и не создавать записи в журнале отката
    protected void insert00(Map<Integer, DbRec> new_rowId_DbRec_Map) {
        int countBeforeAll = rowId_DbRec_Map.size();
        int countForProcessing = new_rowId_DbRec_Map.size();

        for (Map.Entry<Integer, DbRec> entry : new_rowId_DbRec_Map.entrySet()) {
            Integer rowId = entry.getKey();
            DbRec newDbRec = entry.getValue();
            //  После этой вставки нет вставки в журнал отката
            insert000(rowId, newDbRec);
        }

        //  Проверка вставки по количеству записей
        int countAfterAll = rowId_DbRec_Map.size();

        if (countBeforeAll + countForProcessing != countAfterAll) {
            logger.error("countBeforeAll({}) + countForProcessing({}) != countAfterAll({})", countBeforeAll, countForProcessing, countAfterAll);
            logger.error("after insert00: rowId_DbRec_Map = {}", rowId_DbRec_Map);
            throw new RuntimeException("countBeforeAll + countForProcessing != countAfterAll");
        }
    }

    protected List<DbRec> getRows() {
        return rowId_DbRec_Map.values().stream().toList();
    }

    //  Вычисление новых записей (только для UPDATE);
    //  Логирование записей, подлежащих удалению или обновлению;
    //  Удаление этих записей.
    protected void delete00ForDeletingAndUpdating(WhereFunc whereFunc, Map<Integer, DbRec> new_rowId_DbRec_Map, DMLCommandLog dmlCommandLog, UpdateSetCalcFunc updateSetCalcFunc) {
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
        //a(whereFunc, new_rowId_DbRec_Map, dmlCommandLog, updateSetCalcFunc);

        //  2.  Удаление записей, удовлетворяющих where
        delete000(new_rowId_DbRec_Map.keySet());
    }

    //  Создание объекта DbSelect и наполнение его с помощью insert0
    private ResultOfDQLCommand select0(WhereFunc whereFunc) {
        DbSelect dbSelect = new DbSelect(this.dbFields);
        dbSelect.insert0(
                getRows()
                        .stream()
                        .filter(whereFunc::where)
                        .toList()
        );
        return new ResultOfDQLCommand(dbSelect);
    }

    private void insert00(DMLCommandLog dmlCommandLog, List<DbRec> newDbRec_List) {
        for (DbRec newDbRec : newDbRec_List) {
            insert00(dmlCommandLog, newDbRec);
        }
    }

    //  Вычисление ROWID для одной записи и создание записи в журнале отката
    private void insert00(DMLCommandLog dmlCommandLog, DbRec newDbRec) {
        Integer rowId = ++lastInsertedRowId;

        insert000(rowId, newDbRec);

        //  ToDo:   Здесь указываю null, но нужно сделать (иерархию классов) так чтобы null не указывать.
        dmlCommandLog.push(new DMLCommandLogElement(rowId, null));
    }

    //  Непосредственная вставка одной записи в мапу-носитель таблицы без создания записи в журнале отката
    private void insert000(Integer rowId, DbRec newDbRec) {
        if (rowId_DbRec_Map.put(rowId, new DbRec(newDbRec)) != null) {
            throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
        }
    }
}
