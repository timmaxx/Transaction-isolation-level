package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;

public class SQLCommandQueueLogElement {
    private final SQLCommandQueueLogElementType sqlCommandQueueLogElementType;
    private final BaseDbTable baseDbTable;
    private final Integer rowId;
    private final DbRecord oldDbRecord;
    private final DbRecord newDbRecord;

    public SQLCommandQueueLogElement(
            SQLCommandQueueLogElementType sqlCommandQueueLogElementType,
            BaseDbTable baseDbTable,
            Integer rowId,
            DbRecord oldDbRecord,
            DbRecord newDbRecord) {
        this.sqlCommandQueueLogElementType = sqlCommandQueueLogElementType;
        this.baseDbTable = baseDbTable;
        this.rowId = rowId;
        this.oldDbRecord = oldDbRecord;
        this.newDbRecord = newDbRecord;
    }

    public SQLCommandQueueLogElementType getSqlCommandQueueLogElementType() {
        return sqlCommandQueueLogElementType;
    }

    public BaseDbTable getBaseDbTable() {
        return baseDbTable;
    }

    public Integer getRowId() {
        return rowId;
    }
}
