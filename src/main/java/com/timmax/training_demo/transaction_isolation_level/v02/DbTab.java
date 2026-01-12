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
    private ResultOfDMLCommand update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Objects.requireNonNull(whereFunc, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_WHERE_FUNC_INTO_NULL);
        Objects.requireNonNull(updateSetCalcFunc, ERROR_UPDATE_SET_CALC_FUNC_IS_NULL_BUT_YOU_CANNOT_MAKE_IT_NULL);
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        return update0(updateSetCalcFunc, whereFunc);
    }

    //  ToDo:   Сделать этот метод не публичным!
    @Override
    public void rollbackOfInsert(Integer rowId) {
        //  Здесь делается удаление одной строки, но при этом не пишется лог для rollback
        delete000(Set.of(rowId));
    }

    //  ToDo:   Сделать этот метод не публичным!
    @Override
    public void rollbackOfDelete(Integer rowId, DbRec oldDbRec) {
        //  Здесь делается вставка одной строки, но при этом не пишется лог для rollback
        insert00(Map.of(rowId, oldDbRec));
    }

    //  ToDo:   Сделать этот метод не публичным!
    @Override
    public void rollbackOfUpdate(Integer rowId, DbRec oldDbRec) {
        rollbackOfInsert(rowId);
        rollbackOfDelete(rowId, oldDbRec);
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


    //  Будем считать, что SELECT относится к DQL (Data Query Language), но не к DML (Data Manipulation Language).
    //  Вот иерархия для наследников:
    //      -   DML команды (INSERT, UPDATE, DELETE);
    //      -   DQL команда (SELECT).
    public static abstract class SQLCommand {
        protected final DbTab dbTab;

        protected RunnableWithResultOfSQLCommand runnable;

        private final Long millsBeforeRun;


        public SQLCommand(DbTab dbTab) {
            this(0L, dbTab);
        }


        //  Done:   Для этого класса и его наследников, конструкторы с явным параметром
        //          Long millsBeforeRun
        //          лучше сделать не public (protected или package-private),
        //          т.к. в явном виде такие конструкторы нужны только для тестирования
        //          (особенно тестирование для разных уровней изоляции).
        protected SQLCommand(Long millsBeforeRun, DbTab dbTab) {
            this.dbTab = dbTab;
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


    //  Из пакета dql удалил классы DQLCommand и DQLCommandSelect и сделал их внутренними в DbTab.
    //  Это было сделано для того, чтобы методы DbTableLike :: ResultOfDQLCommand select() сделать не публичным,
    //  а это нужно, для того, чтобы выборку можно было делать только из SQLCommandQueue
    //  (т.е. внутри транзакции в дочернем процессе).

    //  Как альтернатива, можно было эти классы перенести в тот-же пакет, что и DbTab. Попробую такой вариант позже.

    //  Но и методы insert, update и delete тоже нужно делать не публичными,
    //  а значит ещё несколько классов в отдельных пакетах нужно удалять и делать внутренними.
    //  Поскольку эти классы стали (станут) внутренними, то передавать DbTab как параметр и иметь переменную класса,
    //  станет не нужно, но только при полном переносе этих классов как внутренние сюда.
    //  Как главный недостаток такого варианта вижу раздувание класса DbTab.


    //  DQL команда (SELECT)
    //      -   не изменяет данные, а значит НЕ порождает журнал изменения,
    //      -   возвращают данные, а значит содержит результат.
    public abstract static class DQLCommand extends SQLCommand {
        public DQLCommand(DbTab dbTab) {
            this(0L, dbTab);
        }


        protected DQLCommand(Long millsBeforeRun, DbTab dbTab) {
            super(millsBeforeRun, dbTab);
        }

        @Override
        final ResultOfDQLCommand run() {
            return (ResultOfDQLCommand)super.run();
        }
    }


    public DQLCommandSelect getDQLCommandSelect(DbTab dbTab) {
        return new DQLCommandSelect(dbTab);
    }

    public DQLCommandSelect getDQLCommandSelect(DbTab dbTab, WhereFunc whereFunc) {
        return new DQLCommandSelect(dbTab, whereFunc);
    }

    public static class DQLCommandSelect extends DQLCommand {
        public DQLCommandSelect(DbTab dbTab) {
            this(0L, dbTab);
        }

        public DQLCommandSelect(DbTab dbTab, WhereFunc whereFunc) {
            this(0L, dbTab, whereFunc);
        }


        protected DQLCommandSelect(Long millsBeforeRun, DbTab dbTab) {
            this(millsBeforeRun, dbTab, dbRec -> true);
        }

        protected DQLCommandSelect(Long millsBeforeRun, DbTab dbTab, WhereFunc whereFunc) {
            super(millsBeforeRun, dbTab);
            runnable = () -> dbTab.select(whereFunc);
        }
    }


    //  Из пакета dml удалил класс DMLCommand и сделал его внутренними в DbTab.
    //  Это было сделано для того, чтобы следующим шагом также поступить с классом SQLCommand
    //  и уже можно будет избавиться от DbTab dbTab как от внутренней переменной,
    //  так и от параметров конструкторов.


    //  DML команды (INSERT, UPDATE, DELETE)
    //      -   могут изменять данные, а значит порождают журнал изменения,
    //      -   не возвращают данные, а значит не содержат результата.
    public static abstract class DMLCommand extends SQLCommand {
        public DMLCommand(DbTab dbTab) {
            this(0L, dbTab);
        }


        protected DMLCommand(Long millsBeforeRun, DbTab dbTab) {
            super(millsBeforeRun, dbTab);
        }

        @Override
        final ResultOfDMLCommand run() {
            return (ResultOfDMLCommand)super.run();
        }
    }

    //  Из пакета dml удалил класс DMLCommandInsert и сделал его внутренними в DbTab.
    //  Это было сделано для того, чтобы методы DbTab :: ResultOfDQLCommand insert() сделать не публичным,
    //  а это нужно, для того, чтобы вставку можно было делать только из SQLCommandQueue
    //  (т.е. внутри транзакции в дочернем процессе).

    public DMLCommandInsert getDMLCommandInsert(DbTab dbTab, DbRec newDbRec) {
        return new DMLCommandInsert(dbTab, newDbRec);
    }

    public DMLCommandInsert getDMLCommandInsert(DbTab dbTab, List<DbRec> newDbRec_List) {
        return new DMLCommandInsert(dbTab, newDbRec_List);
    }

    public static class DMLCommandInsert extends DMLCommand {
        public DMLCommandInsert(DbTab dbTab, DbRec newDbRec) {
            this(0L, dbTab, newDbRec);
        }

        public DMLCommandInsert(DbTab dbTab, List<DbRec> newDbRec_List) {
            this(0L, dbTab, newDbRec_List);
        }


        protected DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, DbRec newDbRec) {
            this(millsBeforeRun, dbTab, List.of(newDbRec));
        }

        protected DMLCommandInsert(Long millsBeforeRun, DbTab dbTab, List<DbRec> newDbRec_List) {
            super(millsBeforeRun, dbTab);
            runnable = () -> dbTab.insert(newDbRec_List);
        }
    }


    //  Из пакета dml удалил класс DMLCommandDelete и сделал его внутренними в DbTab.
    //  Это было сделано для того, чтобы методы DbTab :: ResultOfDQLCommand delete() сделать не публичным,
    //  а это нужно, для того, чтобы удаление можно было делать только из SQLCommandQueue
    //  (т.е. внутри транзакции в дочернем процессе).

    public DMLCommandDelete getDMLCommandDelete(DbTab dbTab) {
        return new DMLCommandDelete(dbTab);
    }

    public DMLCommandDelete getDMLCommandDelete(DbTab dbTab, WhereFunc whereFunc) {
        return new DMLCommandDelete(dbTab, whereFunc);
    }

    public static class DMLCommandDelete extends DMLCommand {
        public DMLCommandDelete(DbTab dbTab) {
            this(0L, dbTab, dbRec -> true);
        }

        public DMLCommandDelete(DbTab dbTab, WhereFunc whereFunc) {
            this(0L, dbTab, whereFunc);
        }


        protected DMLCommandDelete(Long millsBeforeRun, DbTab dbTab) {
            this(millsBeforeRun, dbTab, dbRec -> true);
        }

        protected DMLCommandDelete(Long millsBeforeRun, DbTab dbTab, WhereFunc whereFunc) {
            super(millsBeforeRun, dbTab);
            runnable = () -> dbTab.delete(whereFunc);
        }
    }


    //  Из пакета dml удалил класс DMLCommandUpdate и сделал его внутренними в DbTab.
    //  Это было сделано для того, чтобы методы DbTab :: ResultOfDQLCommand update() сделать не публичным,
    //  а это нужно, для того, чтобы обновление можно было делать только из SQLCommandQueue
    //  (т.е. внутри транзакции в дочернем процессе).

    public DMLCommandUpdate getDMLCommandUpdate(DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc) {
        return new DMLCommandUpdate(dbTab, updateSetCalcFunc);
    }

    public DMLCommandUpdate getDMLCommandUpdate(DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        return new DMLCommandUpdate(dbTab, updateSetCalcFunc, whereFunc);
    }

    public static class DMLCommandUpdate extends DMLCommand {
        public DMLCommandUpdate(DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc) {
            this(0L, dbTab, updateSetCalcFunc);
        }

        public DMLCommandUpdate(DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
            this(0L, dbTab, updateSetCalcFunc, whereFunc);
        }


        protected DMLCommandUpdate(Long millsBeforeRun, DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc) {
            this(millsBeforeRun, dbTab, updateSetCalcFunc, dbRec -> true);
        }

        protected DMLCommandUpdate(Long millsBeforeRun, DbTab dbTab, UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
            super(millsBeforeRun, dbTab);
            runnable = () -> dbTab.update(updateSetCalcFunc, whereFunc);
        }
    }
}
