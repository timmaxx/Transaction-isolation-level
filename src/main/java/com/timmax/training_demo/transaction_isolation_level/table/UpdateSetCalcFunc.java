package com.timmax.training_demo.transaction_isolation_level.table;

@FunctionalInterface
public interface UpdateSetCalcFunc {
    DbRecord setCalcFunc(DbRecord oldDbRecord);
}
