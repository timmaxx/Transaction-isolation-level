package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueueLogElement;

import java.util.Stack;

public class SQLCommandQueueLog {
    Stack<SQLCommandQueueLogElement> sqlCommandQueueLogElementStack = new Stack<>();

    void push(SQLCommandQueueLogElement sqlCommandQueueLogElement) {
        sqlCommandQueueLogElementStack.push(sqlCommandQueueLogElement);
    }

    SQLCommandQueueLogElement pop() {
        return sqlCommandQueueLogElementStack.pop();
    }
}
