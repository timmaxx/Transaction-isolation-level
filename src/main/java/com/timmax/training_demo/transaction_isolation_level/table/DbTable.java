package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.LogAndDataResultOfSQLCommand;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElement;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElementType;

import java.util.Optional;

public class DbTable extends BaseDbTable {
    public DbTable(BaseDbTable baseDbTable) {
        super(baseDbTable);
    }

    @Override
    public LogAndDataResultOfSQLCommand insert(DbRecord newDbRecord) {
        ++rowId;
        insert0(rowId, newDbRecord);
        return new LogAndDataResultOfSQLCommand(
                Optional.of(
                        new SQLCommandQueueLogElement(
                                SQLCommandQueueLogElementType.INSERT,
                                this,
                                rowId,
                                null
                        )
                ),
                Optional.empty()
        );
    }

    @Override
    public void rollback_insert(Integer rowId, DbRecord oldDbRecord) {
        insert0(rowId, oldDbRecord);
    }

    private void insert0(Integer rowId, DbRecord dbRecord) {
        someRecordInDBMap.put(rowId, dbRecord);
    }

    @Override
    public LogAndDataResultOfSQLCommand update(Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        if (!someRecordInDBMap.containsKey(rowId)) {
            return new LogAndDataResultOfSQLCommand(Optional.empty(), Optional.empty());
        }

        DbRecord oldDbRecord = someRecordInDBMap.get(rowId);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        DbRecord newDbRecord = updateSetCalcFunc.setCalcFunc(oldDbRecord);

        update0(rowId, newDbRecord);

        return new LogAndDataResultOfSQLCommand(
                Optional.of(
                        new SQLCommandQueueLogElement(
                                SQLCommandQueueLogElementType.UPDATE,
                                this,
                                rowId,
                                oldDbRecord
                        )
                ),
                Optional.empty()
        );
    }

    @Override
    public void rollback_update(Integer rowId, DbRecord oldDbRecord) {
        update0(rowId, oldDbRecord);
    }

    private void update0(Integer rowId, DbRecord dbRecord) {
        someRecordInDBMap.put(rowId, dbRecord);
    }

    @Override
    public LogAndDataResultOfSQLCommand delete(Integer rowId) {
        if (!someRecordInDBMap.containsKey(rowId)) {
            return new LogAndDataResultOfSQLCommand(Optional.empty(), Optional.empty());
        }
        DbRecord oldDbRecord = someRecordInDBMap.get(rowId);
        delete0(rowId);

        return new LogAndDataResultOfSQLCommand(
                Optional.of(
                        new SQLCommandQueueLogElement(
                                SQLCommandQueueLogElementType.DELETE,
                                this,
                                rowId,
                                oldDbRecord
                        )
                ),
                Optional.empty()
        );
    }

    @Override
    public void rollback_delete(Integer rowId) {
        delete0(rowId);
    }

    private void delete0(Integer rowId) {
        someRecordInDBMap.remove(rowId);
    }
}
