package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;

public class SQLCommandQueueLogElement {
    private final SQLCommandQueueLogElementType sqlCommandQueueLogElementType;
    private final BaseDbTable baseDbTable;
    private final Integer rowId;
    private final DbRecord beforeDbRecord;
    private final DbRecord afterDbRecord;

    public SQLCommandQueueLogElement(
            SQLCommandQueueLogElementType sqlCommandQueueLogElementType,
            BaseDbTable baseDbTable,
            Integer rowId,
            DbRecord beforeDbRecord,
            DbRecord afterDbRecord) {
        this.sqlCommandQueueLogElementType = sqlCommandQueueLogElementType;
        this.baseDbTable = baseDbTable;
        this.rowId = rowId;
        this.beforeDbRecord = beforeDbRecord;
        this.afterDbRecord = afterDbRecord;
    }
}
