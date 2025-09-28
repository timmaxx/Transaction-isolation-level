package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;
import java.util.Objects;

public class DbRec {
    private final Map<String, Object> rec;

    public DbRec(Map<String, Object> rec) {
        this.rec = rec;
    }

    public int size() {
        return rec.size();
    }

    @Override
    public String toString() {
        return "DbRec{" +
                "rec=" + rec +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbRec dbRec)) return false;
        return Objects.equals(rec, dbRec.rec);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rec);
    }
}
