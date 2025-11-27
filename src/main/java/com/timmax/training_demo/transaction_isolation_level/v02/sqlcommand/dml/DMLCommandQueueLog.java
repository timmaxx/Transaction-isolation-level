package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import java.util.Stack;

public class DMLCommandQueueLog {
    Stack<DMLCommandQueueLog> dmlCommandQueueLog_Stack = new Stack<>();

    public void push(DMLCommandQueueLog dmlCommandQueueLog) {
        dmlCommandQueueLog_Stack.push(dmlCommandQueueLog);
    }
/*
    DMLCommandQueueLog pop() {
        return dmlCommandQueueLog_Stack.pop();
    }
*/
}
