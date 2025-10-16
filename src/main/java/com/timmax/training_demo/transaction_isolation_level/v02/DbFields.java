package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DbFields {
    private final Map<DbFieldName, Class<?>> dbFields;

    //  Warning:(10, 21) Raw use of parameterized class 'DbField'
    public DbFields(DbField... dbFields) {
        this.dbFields = new HashMap<>();
        //  Warning:(12, 14) Raw use of parameterized class 'DbField'
        for (DbField dbField : dbFields) {
            this.dbFields.put(dbField.getDbFieldName(), dbField.getType());
        }
    }

    public Class<?> getDbFieldType(DbFieldName dbFieldName) {
        return dbFields.get(dbFieldName);
    }

    public boolean containsKey(DbFieldName dbFieldName) {
        return dbFields.containsKey(dbFieldName);
    }

    @Override
    public String toString() {
        return "DbFields{" +
                "dbFields=" + dbFields +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbFields dbFields1)) return false;
        return Objects.equals(dbFields, dbFields1.dbFields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbFields);
    }

    public Map<DbFieldName, Class<?>> getDbFields() {
        return dbFields;
    }
}
