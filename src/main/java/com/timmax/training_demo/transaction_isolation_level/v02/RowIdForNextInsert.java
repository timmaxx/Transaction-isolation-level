package com.timmax.training_demo.transaction_isolation_level.v02;

public class RowIdForNextInsert extends RowId {

    public RowIdForNextInsert(Integer id) {
        super(id);
    }

    RowId generateAndGetNext() {
        ++id;
        return this;
    }
}
