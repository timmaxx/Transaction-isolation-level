package com.timmax.training_demo.transaction_isolation_level.table;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElement;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElementType;

import java.util.Optional;

public class DbTable extends BaseDbTable {
    public DbTable(BaseDbTable baseDbTable) {
        super(baseDbTable);
    }

    @Override
    public Optional<SQLCommandQueueLogElement> insert(DbRecord newDbRecord) {
        ++rowId;
        insert0(rowId, newDbRecord);
        return Optional.of(
                new SQLCommandQueueLogElement(
                        SQLCommandQueueLogElementType.INSERT,
                        this,
                        rowId,
                        null,
                        newDbRecord
                )
        );
    }

    private void insert0(Integer rowId, DbRecord dbRecord) {
        someRecordInDBMap.put(rowId, dbRecord);
    }

    @Override
    public Optional<SQLCommandQueueLogElement> update(Integer rowId, UpdateSetCalcFunc updateSetCalcFunc) {
        if (!someRecordInDBMap.containsKey(rowId)) {
            return Optional.empty();
        }

        DbRecord oldDbRecord = someRecordInDBMap.get(rowId);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        DbRecord newDbRecord = updateSetCalcFunc.setCalcFunc(oldDbRecord);

        update0(rowId, newDbRecord);

        return Optional.of(
                new SQLCommandQueueLogElement(
                        SQLCommandQueueLogElementType.UPDATE,
                        this,
                        rowId,
                        oldDbRecord,
                        newDbRecord)
        );
    }

    private void update0(Integer rowId, DbRecord dbRecord) {
        someRecordInDBMap.put(rowId, dbRecord);
    }

    @Override
    public void delete(Integer rowId) {
        if (!someRecordInDBMap.containsKey(rowId)) {
            return;
        }
        delete0(rowId);
    }

    private void delete0(Integer rowId) {
        someRecordInDBMap.remove(rowId);
    }
}
