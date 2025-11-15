package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql;

import com.timmax.training_demo.transaction_isolation_level.v02.DbSelect;

import java.util.Stack;

public class DQLResultLog {
    Stack<DbSelect> dbSelectStack = new Stack<>();

    public void push(DbSelect dbSelect) {
        dbSelectStack.push(dbSelect);
    }

    public DbSelect pop() {
        return dbSelectStack.pop();
    }
}
