package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.HashMap;
import java.util.Map;

public class DbFields {
    private final Map<String, Class<?>> fields = new HashMap<>();

    void putField(DbField dbField) {
        fields.put(dbField.getName(), dbField.getType());
    }

    @Override
    public String toString() {
        return "DbFields{" +
                "fields=" + fields +
                '}';
    }
}
