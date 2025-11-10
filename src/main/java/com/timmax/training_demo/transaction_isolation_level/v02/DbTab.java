package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public non-sealed class DbTab extends DbTableLike {
    protected static final Logger logger = LoggerFactory.getLogger(DbTab.class);

    private static final String ERROR_TABLE_IS_RO = "ERROR: The table '%s' is read only.";
    private static final String YOU_CANNOT_INSERT = "You cannot insert any row into this table.";
    private static final String YOU_CANNOT_UPDATE = "You cannot update any row in this table.";
    private static final String YOU_CANNOT_DELETE = "You cannot delete any row from this table.";

    static final String ERROR_TABLE_IS_RO_YOU_CANNOT_INSERT = ERROR_TABLE_IS_RO + " " + YOU_CANNOT_INSERT;
    static final String ERROR_TABLE_IS_RO_YOU_CANNOT_UPDATE = ERROR_TABLE_IS_RO + " " + YOU_CANNOT_UPDATE;
    static final String ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE = ERROR_TABLE_IS_RO + " " + YOU_CANNOT_DELETE;

    private final DbTabName dbTabName;
    private final boolean readOnly;

    public DbTab(DbTabName dbTabName, DbFields dbFields, boolean readOnly) {
        super(dbFields);
        this.dbTabName = dbTabName;
        this.readOnly = readOnly;
    }

    public DbTab(DbTab dbTab, boolean readOnly) {
        this(dbTab.dbTabName, dbTab.dbFields, readOnly);
        this.dbRecs.addAll(
                dbTab.dbRecs.stream()
                        .map(DbRec::new)
                        .toList()
        );
    }

    public DbTab(DbTab dbTab, boolean readOnly, Set<DbRec> dbRecs) {
        this(dbTab, readOnly);
        this.dbRecs.addAll(
                dbRecs.stream()
                        .map(DbRec::new)
                        .toList()
        );
    }

    private void validateReadOnlyTable(String msgInsUpdDel) {
        if (!readOnly) {
            return;
        }
        throw new DbDataAccessException(String.format(ERROR_TABLE_IS_RO, dbTabName) + " " + msgInsUpdDel);
    }

    public void insert(DbRec newDbRec) {
        validateReadOnlyTable(YOU_CANNOT_INSERT);
        insert0(newDbRec);
    }

    public void delete() {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        delete0();
    }

    public void delete(WhereFunc whereFunc) {
        validateReadOnlyTable(YOU_CANNOT_DELETE);
        delete0(whereFunc);
    }

    private void delete0() {
        dbRecs.clear();
    }

    private void delete0(WhereFunc whereFunc) {
        dbRecs.removeIf(whereFunc::where);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc) {
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        update0(updateSetCalcFunc, null);
    }

    public void update(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        validateReadOnlyTable(YOU_CANNOT_UPDATE);
        update0(updateSetCalcFunc, whereFunc);
    }

    private void update0(UpdateSetCalcFunc updateSetCalcFunc, WhereFunc whereFunc) {
        Set<DbRec> oldDbRecs = new HashSet<>();
        Set<DbRec> newDbRecs = new HashSet<>();
        int beforeCount = dbRecs.size();

        for (DbRec oldDbRec : dbRecs) {
            if (whereFunc == null || whereFunc.where(oldDbRec)) {
                oldDbRecs.add(oldDbRec);
                //  Берём все поля из старой записи и переписываем те, которые поступили ч/з функцию setCalcFunc.
                DbRec newDbRec = new DbRec(oldDbRec, updateSetCalcFunc.setCalcFunc(oldDbRec));
                if (dbRecs.contains(newDbRec) ||
                        newDbRecs.contains(newDbRec)
                ) {
                    throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
                }
                if (!newDbRecs.add(newDbRec)) {
                    throw new DbSQLException(ERROR_DUPLICATE_KEY_VALUE_VIOLATES_UNIQUE_CONSTRAINT_COMBINATIONS_OF_ALL_FIELDS_MUST_BE_UNIQUE);
                }
            }
        }

        int oldCount = oldDbRecs.size();
        int newCount = newDbRecs.size();

        if (oldCount != newCount) {
            logger.error("oldCount != newCount");
            logger.error("beforeCount = {}, oldCount = {}, newCount = {}", beforeCount, oldCount, newCount);
            throw new RuntimeException("oldCount != newCount");
        }

        dbRecs.removeAll(oldDbRecs);
        int afterRemoveCount = dbRecs.size();

        if (beforeCount - oldCount != afterRemoveCount) {
            logger.error("beforeCount - oldCount != afterRemoveCount");
            logger.error("oldTestRecordSet = {}", oldDbRecs);
            logger.error("newTestRecordSet = {}", newDbRecs);
            logger.error("beforeCount = {}, oldCount = {}, newCount = {}", beforeCount, oldCount, newCount);
            logger.error("afterRemoveCount = {}", afterRemoveCount);
            logger.error("after remove:  testRecordSet = {}", dbRecs);
            throw new RuntimeException("beforeCount - oldCount != afterRemoveCount");
        }

        insert0(newDbRecs);
        int afterCount = dbRecs.size();

        if (afterRemoveCount + newCount != afterCount) {
            logger.error("afterRemoveCount + newCount != afterCount");
            logger.error("afterRemoveCount = {}, newCount = {}, afterCount = {}", afterRemoveCount, newCount, afterCount);
            logger.error("after insert0: testRecordSet = {}", dbRecs);
            throw new RuntimeException("afterRemoveCount + newCount != afterCount");
        }
    }

    @Override
    public String toString() {
        return "DbTab{" +
                "dbTabName='" + dbTabName + '\'' +
                ", readOnly=" + readOnly +
                ", dbFields=" + dbFields +
                ", dbRecs=" + dbRecs +
                '}';
    }
}
