package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;

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
}
