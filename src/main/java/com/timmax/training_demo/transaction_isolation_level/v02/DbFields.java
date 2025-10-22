package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.*;

public class DbFields {
    private final Map<DbFieldName, Class<?>> dbFields = new LinkedHashMap<>();

    //  Warning:(9, 21) Raw use of parameterized class 'DbField'
    public DbFields(DbField... arrayOfDbFields) {
        Arrays.stream(arrayOfDbFields)
                .forEach(dbField -> {
                    if (dbFields.containsKey(dbField.getDbFieldName())) {
                        throw new RuntimeException("Duplicate key: " + dbField.getDbFieldName());
                    }
                    if (dbField.getType() == null) {
                        throw new RuntimeException("Type is null: " + dbField.getDbFieldName());
                    }
                    dbFields.put(dbField.getDbFieldName(), dbField.getType());
                });
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
