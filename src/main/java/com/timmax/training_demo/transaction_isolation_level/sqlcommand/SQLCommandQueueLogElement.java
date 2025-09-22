package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.BaseDbTable;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;

public record SQLCommandQueueLogElement(
        SQLCommandQueueLogElementType sqlCommandQueueLogElementType,
        BaseDbTable baseDbTable,
        Integer rowId,
        DbRecord oldDbRecord) {
}
