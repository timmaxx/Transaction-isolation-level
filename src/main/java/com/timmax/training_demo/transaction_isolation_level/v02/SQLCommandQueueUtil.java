package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Arrays;

public class SQLCommandQueueUtil {
    public static void startAllAndJoinToAllThreads(SQLCommandQueue... sqlCommandQueueArray) {
        Arrays.stream(sqlCommandQueueArray).forEach(SQLCommandQueue::startThread);
        Arrays.stream(sqlCommandQueueArray).forEach(SQLCommandQueue::joinToThread);
    }
}
