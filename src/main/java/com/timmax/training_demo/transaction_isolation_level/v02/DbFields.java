package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DbFields {
    private final Map<String, Class<?>> fields;

    public DbFields(DbField... dbFields) {
        fields = new HashMap<>();
        for (DbField dbField : dbFields) {
            fields.put(dbField.getName(), dbField.getType());
        }
    }

    @Override
    public String toString() {
        return "DbFields{" +
                "fields=" + fields +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbFields dbFields)) return false;
        return Objects.equals(fields, dbFields.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fields);
    }
}
