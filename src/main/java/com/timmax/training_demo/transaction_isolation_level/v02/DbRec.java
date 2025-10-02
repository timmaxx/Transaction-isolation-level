package com.timmax.training_demo.transaction_isolation_level.v02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DbRec {
    protected static final Logger logger = LoggerFactory.getLogger(DbRec.class);

    private final Map<DbFieldName, Object> recMap;

    public DbRec() {
        recMap = new HashMap<>();
    }

    public DbRec(Map<DbFieldName, Object> recMap) {
        this();
        //  for (Map.Entry<DbFieldName, Object> entry : recMap.entrySet()) {
        //      this.recMap.put(entry.getKey(), entry.getValue());
        //  }
        this.recMap.putAll(recMap);
    }

    public DbRec(DbRec rec) {
        this(rec.recMap);
    }

    public void setAll(DbRec rec) {
        for (Map.Entry<DbFieldName, Object> entry : recMap.entrySet()) {
            if (!entry.getValue().equals(rec.getValue(entry.getKey()))) {
                entry.setValue(rec.getValue(entry.getKey()));
            }
        }
    }

    public int size() {
        return recMap.size();
    }

    public Object getValue(DbFieldName fieldName) {
        return recMap.get(fieldName);
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
