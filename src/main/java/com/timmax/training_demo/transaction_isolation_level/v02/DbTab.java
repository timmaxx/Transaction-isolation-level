package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.ResultOfSQLCommand;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.RunnableWithResultOfSQLCommand;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandLog;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.ResultOfDMLCommand;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.ResultOfDQLCommand;

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
        insert0(dbTab.getRows());
    }

    public DbTab(DbTab dbTab, boolean readOnly, List<DbRec> dbRec_List) {
        this(dbTab, readOnly);
        insert0(dbRec_List);
    }


    public DQLCommandSelect getDQLCommandSelect() {
        return new DQLCommandSelect();
    }

    public DQLCommandSelect getDQLCommandSelect(WhereFunc whereFunc) {
        return new DQLCommandSelect(whereFunc);
    }

    public DMLCommandInsert getDMLCommandInsert(DbRec newDbRec) {
        return new DMLCommandInsert(newDbRec);
    }

    public DMLCommandInsert getDMLCommandInsert(List<DbRec> newDbRec_List) {
        return new DMLCommandInsert(newDbRec_List);
    }

    public DMLCommandDelete getDMLCommandDelete() {
        return new DMLCommandDelete();
    }

    public DMLCommandDelete getDMLCommandDelete(WhereFunc whereFunc) {
        return new DMLCommandDelete(whereFunc);
    }

    public DMLCommandUpdate getDMLCommandUpdate(UpdateSetCalcFunc updateSetCalcFunc) {
        return new DMLCommandUpdate(updateSetCalcFunc);
    }

    public DMLCommandUpdate getDMLCommandUpdate(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        return new DMLCommandUpdate(updateSetCalcFunc, whereFunc);
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "dbTabName='" + dbTabName + '\'' +
                ", readOnly=" + readOnly +
                ", dbFields=" + dbFields +
                '}';
    }


    DQLCommandSelect getDQLCommandSelect(Long millsBeforeRun) {
        return new DQLCommandSelect(millsBeforeRun);
    }

    DMLCommandInsert getDMLCommandInsert(Long millsBeforeRun, DbRec newDbRec) {
        return new DMLCommandInsert(millsBeforeRun, newDbRec);
    }

    DMLCommandUpdate getDMLCommandUpdate(Long millsBeforeRun, Long millsInsideUpdate, UpdateSetCalcFunc updateSetCalcFunc) {
        return new DMLCommandUpdate(millsBeforeRun, millsInsideUpdate, updateSetCalcFunc);
    }

    @Override
    void rollbackOfInsert(Integer rowId) {
        //  Здесь делается удаление одной строки, но при этом не пишется лог отката
        delete000(Set.of(rowId));
    }

    @Override
    void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        //  Здесь делается вставка одной строки, но при этом не пишется лог отката
        insert00(Map.of(rowId, oldDbRec));
    }

    @Override
    void rollbackOfUpdate(Integer rowId, DbRec oldDbRec) {
        rollbackOfInsert(rowId);
        rollbackOfDelete(rowId, oldDbRec);
    }


    //  INSERT списка записей
    private ResultOfDMLCommand insert(List<DbRec> newDbRec_List) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        return insert0(newDbRec_List);
    }

    //  DELETE выборочных записей (с WHERE)
    private ResultOfDMLCommand delete(WhereFunc whereFunc) {
        Objects.requireNonNull(whereFunc, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        return delete0(whereFunc);
    }

    //  UPDATE выборочных записей (с WHERE)
    private ResultOfDMLCommand update(Long millsInsideUpdate, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Objects.requireNonNull(whereFunc, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        Objects.requireNonNull(updateSetCalcFunc, ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL);
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        return update0(millsInsideUpdate, updateSetCalcFunc, whereFunc);
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
        delete00ForDeletingAndUpdating(0L, whereFunc, new_rowId_DbRec_Map, dmlCommandLog, null);

        //  Здесь нет кода, который есть в update (т.е. вставки)

        return new ResultOfDMLCommand(dmlCommandLog);
    }

    private ResultOfDMLCommand update0(Long millsInsideUpdate, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Map<Integer, DbRec> new_rowId_DbRec_Map = new HashMap<>();
        DMLCommandLog dmlCommandLog = new DMLCommandLog(this, UPDATE);

        //  Для UPDATE updateSetCalcFunc передаём в метод
        delete00ForDeletingAndUpdating(millsInsideUpdate, whereFunc, new_rowId_DbRec_Map, dmlCommandLog, updateSetCalcFunc);

        //  Наличием этого кода отличается от
        //  private ResultOfDMLCommand delete0(WhereFunc whereFunc)
        //  Вставляем в основную таблицу те записи, которые были созданы выше в new_rowId_DbRec_Map
        insert00(new_rowId_DbRec_Map);

        return new ResultOfDMLCommand(dmlCommandLog);
    }


    //  Удалил классы и сделал их внутренними в классе DbTab:
    //  - из пакета sqlcommand класс SQLCommand,
    //  - из пакета sqlcommand\dql классы DQLCommand и DQLCommandSelect,
    //  - из пакета sqlcommand\dml классы DMLCommand, DMLCommandInsert, DMLCommandUpdate, DMLCommandDelete.
    //  Это было сделано для того, чтобы методы
    //  - DbTableLike :: ResultOfDQLCommand select() и
    //  - DbTab :: insert(), update() и delete()
    //  сделать не публичным, а это нужно, для того, чтобы выборку, вставку, обновление и удаление
    //  можно было делать только из SQLCommandQueue (т.е. внутри транзакции в дочернем процессе).

    //  Как альтернатива, можно было эти классы перенести в тот-же пакет, что и DbTab.

    //  Как главный недостаток такого варианта вижу раздувание класса DbTab.


    //  Будем считать, что SELECT относится к DQL (Data Query Language), но не к DML (Data Manipulation Language).
    //  Вот иерархия для наследников:
    //      -   DML команды (INSERT, UPDATE, DELETE);
    //      -   DQL команда (SELECT).
    public abstract static class SQLCommand {
        protected RunnableWithResultOfSQLCommand runnable;

        private final Long millsBeforeRun;


        public SQLCommand() {
            this(0L);
        }


        //  Done:   Для этого класса и его наследников, конструкторы с явным параметром
        //          Long millsBeforeRun
        //          лучше сделать не public (protected или package-private),
        //          т.к. в явном виде такие конструкторы нужны только для тестирования
        //          (особенно тестирование для разных уровней изоляции).
        protected SQLCommand(Long millsBeforeRun) {
            this.millsBeforeRun = millsBeforeRun;
        }

        ResultOfSQLCommand run() {
            if (millsBeforeRun > 0) {
                try {
                    Thread.sleep(millsBeforeRun);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return runnable.run();
        }
    }


    //  DQL команда (SELECT)
    //      -   не изменяет данные, а значит НЕ порождает журнал изменения,
    //      -   возвращают данные, а значит содержит результат.
    private abstract static class DQLCommand extends SQLCommand {
        public DQLCommand() {
            this(0L);
        }


        protected DQLCommand(Long millsBeforeRun) {
            super(millsBeforeRun);
        }

        @Override
        final ResultOfDQLCommand run() {
            return (ResultOfDQLCommand)super.run();
        }
    }


    public class DQLCommandSelect extends DQLCommand {
        public DQLCommandSelect() {
            this(0L);
        }

        public DQLCommandSelect(WhereFunc whereFunc) {
            this(0L, whereFunc);
        }


        protected DQLCommandSelect(Long millsBeforeRun) {
            this(millsBeforeRun, dbRec -> true);
        }

        protected DQLCommandSelect(Long millsBeforeRun, WhereFunc whereFunc) {
            super(millsBeforeRun);
            runnable = () -> select(whereFunc);
        }
    }


    //  DML команды (INSERT, UPDATE, DELETE)
    //      -   могут изменять данные, а значит порождают журнал изменения,
    //      -   не возвращают данные, а значит не содержат результата.
    private abstract static class DMLCommand extends SQLCommand {
        public DMLCommand() {
            this(0L);
        }


        protected DMLCommand(Long millsBeforeRun) {
            super(millsBeforeRun);
        }

        @Override
        final ResultOfDMLCommand run() {
            return (ResultOfDMLCommand)super.run();
        }
    }


    public class DMLCommandInsert extends DMLCommand {
        public DMLCommandInsert(DbRec newDbRec) {
            this(0L, newDbRec);
        }

        public DMLCommandInsert(List<DbRec> newDbRec_List) {
            this(0L, newDbRec_List);
        }


        protected DMLCommandInsert(Long millsBeforeRun, DbRec newDbRec) {
            this(millsBeforeRun, List.of(newDbRec));
        }

        protected DMLCommandInsert(Long millsBeforeRun, List<DbRec> newDbRec_List) {
            super(millsBeforeRun);
            runnable = () -> insert(newDbRec_List);
        }
    }


    public class DMLCommandDelete extends DMLCommand {
        public DMLCommandDelete() {
            this(0L, dbRec -> true);
        }

        public DMLCommandDelete(WhereFunc whereFunc) {
            this(0L, whereFunc);
        }


        protected DMLCommandDelete(Long millsBeforeRun) {
            this(millsBeforeRun, dbRec -> true);
        }

        protected DMLCommandDelete(Long millsBeforeRun, WhereFunc whereFunc) {
            super(millsBeforeRun);
            runnable = () -> delete(whereFunc);
        }
    }


    public class DMLCommandUpdate extends DMLCommand {
        public DMLCommandUpdate(UpdateSetCalcFunc updateSetCalcFunc) {
            this(0L, 0L, updateSetCalcFunc);
        }

        public DMLCommandUpdate(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
            this(0L, 0L, updateSetCalcFunc, whereFunc);
        }


        protected DMLCommandUpdate(Long millsBeforeRun, Long millsInsideUpdate, UpdateSetCalcFunc updateSetCalcFunc) {
            this(millsBeforeRun, millsInsideUpdate, updateSetCalcFunc, dbRec -> true);
        }

        protected DMLCommandUpdate(Long millsBeforeRun, Long millsInsideUpdate, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
            super(millsBeforeRun);
            runnable = () -> update(millsInsideUpdate, updateSetCalcFunc, whereFunc);
        }
    }
}
