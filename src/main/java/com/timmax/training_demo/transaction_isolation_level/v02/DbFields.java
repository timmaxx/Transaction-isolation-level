package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DbFields {
    final static String ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE = "ERROR: column '%s' specified more than once.";
    final static String ERROR_TYPE_DOES_NOT_EXIST = "ERROR: type '%s' does not exist ('%s').";

    private final Map<DbFieldName, Class<?>> dbFieldNameClassMap = new LinkedHashMap<>();

    //  ToDo:
    //  Warning:(9, 21) Raw use of parameterized class 'DbField'
    public DbFields(DbField... arrayOfDbFields) {
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        Arrays.stream(arrayOfDbFields)
                .forEach(dbField -> {
                    if (dbFieldNameClassMap.containsKey(dbField.getDbFieldName())) {
                        sb.append(String.format(ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE, dbField.getDbFieldName())).append("\n");
                        isThereError.set(true);
                    }
                    if (dbField.getClazz() == null) {
                        sb.append(String.format(ERROR_TYPE_DOES_NOT_EXIST, "null", dbField.getDbFieldName())).append("\n");
                        isThereError.set(true);
                    }
                    if (!isThereError.get()) {
                        dbFieldNameClassMap.put(dbField.getDbFieldName(), dbField.getClazz());
                    }
                });
        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
    }

    public Class<?> getDbFieldType(DbFieldName dbFieldName) {
        return dbFieldNameClassMap.get(dbFieldName);
    }

    public boolean containsKey(DbFieldName dbFieldName) {
        return dbFieldNameClassMap.containsKey(dbFieldName);
    }

    @Override
    public String toString() {
        return "DbFields{" +
                "dbFieldNameClassMap=" + dbFieldNameClassMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbFields dbFields1)) return false;
        return Objects.equals(dbFieldNameClassMap, dbFields1.dbFieldNameClassMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbFieldNameClassMap);
    }

    public Map<DbFieldName, Class<?>> getDbFieldNameClassMap() {
        return dbFieldNameClassMap;
    }
}
