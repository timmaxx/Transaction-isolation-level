package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import java.util.Stack;

public class DMLCommandQueueLog {
    Stack<DMLCommandLog> dmlCommandQueueLog_Stack = new Stack<>();

    public void push(DMLCommandLog dmlCommandLog) {
        dmlCommandQueueLog_Stack.push(dmlCommandLog);
    }

    public DMLCommandLog pop() {
        return dmlCommandQueueLog_Stack.pop();
    }
}
