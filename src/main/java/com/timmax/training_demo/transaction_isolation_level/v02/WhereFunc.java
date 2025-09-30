package com.timmax.training_demo.transaction_isolation_level.v02;

@FunctionalInterface
public interface WhereFunc {
    boolean where(DbRec dbRec);
}
