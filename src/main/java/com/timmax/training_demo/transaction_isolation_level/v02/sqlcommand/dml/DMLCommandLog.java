package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTableLike;

import java.util.Stack;

public class DMLCommandLog {
    private final DbTableLike dbTabLike;
    private final DMLCommandLogElementType dmlCommandLogElementType;
    private final Stack<DMLCommandLogElement> dmlCommandQueueLogElement_Stack = new Stack<>();

    public DMLCommandLog(DbTableLike dbTabLike, DMLCommandLogElementType dmlCommandLogElementType) {
        this.dbTabLike = dbTabLike;
        this.dmlCommandLogElementType = dmlCommandLogElementType;
    }

    public DbTableLike getDbTabLike() {
        return dbTabLike;
    }

    public DMLCommandLogElementType getDmlCommandLogElementType() {
        return dmlCommandLogElementType;
    }

    public void push(DMLCommandLogElement dmlCommandLogElement) {
        dmlCommandQueueLogElement_Stack.push(dmlCommandLogElement);
    }

    public DMLCommandLogElement pop() {
        return dmlCommandQueueLogElement_Stack.pop();
    }
}
