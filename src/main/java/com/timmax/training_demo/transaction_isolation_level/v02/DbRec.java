package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;
import java.util.Objects;

public class DbRec {
    private final Map<DbFieldName, Object> recMap;

    public DbRec(Map<DbFieldName, Object> recMap) {
        this.recMap = recMap;
    }

    public DbRec(DbRec rec) {
        this(rec.recMap);
    }

    public int size() {
        return recMap.size();
    }

    @Override
    public String toString() {
        return "DbRec{" +
                "recMap=" + recMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbRec dbRec)) return false;
        return Objects.equals(recMap, dbRec.recMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(recMap);
    }
}
