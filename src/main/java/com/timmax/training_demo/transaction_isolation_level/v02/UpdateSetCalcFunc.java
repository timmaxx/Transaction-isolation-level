package com.timmax.training_demo.transaction_isolation_level.v02;

@FunctionalInterface
public interface UpdateSetCalcFunc {
    DbRec setCalcFunc(DbRec oldDbRec);
}
