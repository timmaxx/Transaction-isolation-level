package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;

import java.util.Stack;

public class ImmutableDbTableResultLog {
    Stack<ImmutableDbTable> dbRecordStack = new Stack<>();

    void push(ImmutableDbTable immutableDbTable) {
        dbRecordStack.push(immutableDbTable);
    }

    ImmutableDbTable pop() {
        return dbRecordStack.pop();
    }
}
