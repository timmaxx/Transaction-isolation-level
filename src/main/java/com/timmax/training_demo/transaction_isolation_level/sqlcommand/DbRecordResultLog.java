package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;

import java.util.Stack;

public class DbRecordResultLog {
    Stack<DbRecord> dbRecordStack = new Stack<>();

    void push(DbRecord dbRecord) {
        dbRecordStack.push(dbRecord);
    }

    DbRecord pop() {
        return dbRecordStack.pop();
    }
}
