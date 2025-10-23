package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DbFields {
    final static String ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE = "ERROR: column '%s' specified more than once.";
    final static String ERROR_TYPE_DOES_NOT_EXIST = "ERROR: type '%s' does not exist ('%s').";

    private final Map<DbFieldName, Class<?>> dbFields = new LinkedHashMap<>();

    //  ToDo:
    //  Warning:(9, 21) Raw use of parameterized class 'DbField'
    public DbFields(DbField... arrayOfDbFields) {
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        Arrays.stream(arrayOfDbFields)
                .forEach(dbField -> {
                    if (dbFields.containsKey(dbField.getDbFieldName())) {
                        sb.append(String.format(ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE, dbField.getDbFieldName())).append("\n");
                        isThereError.set(true);
                    }
                    if (dbField.getType() == null) {
                        sb.append(String.format(ERROR_TYPE_DOES_NOT_EXIST, "null", dbField.getDbFieldName())).append("\n");
                        isThereError.set(true);
                    }
                    if (!isThereError.get()) {
                        dbFields.put(dbField.getDbFieldName(), dbField.getType());
                    }
                });
        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
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
