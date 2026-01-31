package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Objects;

public class RowId {
    protected Integer id;

    protected static final String ERROR_INNER_TROUBLE_YOU_CANNOT_SET_ROW_ID_INTO_NULL = "ERROR: Inner trouble. You cannot set rowId into null!";


    RowId(Integer id) {
        Objects.requireNonNull(id, ERROR_INNER_TROUBLE_YOU_CANNOT_SET_ROW_ID_INTO_NULL);
        this.id = id;
    }

    RowId(RowId rowId) {
        this(rowId.id);
    }

    @Override
    public String toString() {
        return "RowId{" +
                "id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RowId rowId)) {
            return false;
        }
        return Objects.equals(id, rowId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
