package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;

@FunctionalInterface
public interface UpdateSetCalcFunc {
    Map<DbFieldName, Object> setCalcFunc(DbRec oldDbRec);
}
