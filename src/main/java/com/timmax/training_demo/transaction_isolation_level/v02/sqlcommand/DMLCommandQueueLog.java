package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandQueueLogElement;

import java.util.Stack;

public class DMLCommandQueueLog {
    Stack<DMLCommandQueueLogElement> dmlCommandQueueLogElementStack = new Stack<>();

    void push(DMLCommandQueueLogElement dmlCommandQueueLogElement) {
        dmlCommandQueueLogElementStack.push(dmlCommandQueueLogElement);
    }

    DMLCommandQueueLogElement pop() {
        return dmlCommandQueueLogElementStack.pop();
    }
}
