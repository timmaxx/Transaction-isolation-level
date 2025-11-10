package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DbFields {
    final static String ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE = "ERROR: column '%s' specified more than once.";
    final static String ERROR_TYPE_DOES_NOT_EXIST = "ERROR: type '%s' does not exist ('%s').";

    private final Map<DbFieldName, DbFieldDefinition<?>> dbFieldName_DbFieldDefinition_Map = new LinkedHashMap<>();

    //  ToDo:   Смотри комментарии к методам DbRec :: verify...
    //  Warning:(9, 21) Raw use of parameterized class 'DbField'
    public DbFields(DbField... arrayOfDbFields) {
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        Arrays.stream(arrayOfDbFields)
                .forEach(dbField -> {
                    if (dbFieldName_DbFieldDefinition_Map.containsKey(dbField.getDbFieldName())) {
                        sb.append(String.format(ERROR_COLUMN_SPECIFIED_MORE_THAN_ONCE, dbField.getDbFieldName())).append("\n");
                        isThereError.set(true);
                    }
                    //  ToDo:   Отрефакторить проверку на допустимый тип поля.
                    //          Здесь "типа проверяется на допустимый тип поля".
                    //          Но он в принципе не должен быть null.
                    //          Пока проверка на null есть, что-бы имитировать проверку типа.
                    if (dbField.getDbFieldDefinition().getClazz() == null) {
                        sb.append(String.format(ERROR_TYPE_DOES_NOT_EXIST, "null", dbField.getDbFieldName())).append("\n");
                        isThereError.set(true);
                    }
                    if (!isThereError.get()) {
                        dbFieldName_DbFieldDefinition_Map.put(dbField.getDbFieldName(), dbField.getDbFieldDefinition());
                    }
                });
        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
    }

    //  Warning:(43, 13) Raw use of parameterized class 'DbFieldDefinition'
    private DbFieldDefinition getDbFieldDefinition(DbFieldName dbFieldName) {
        return dbFieldName_DbFieldDefinition_Map.get(dbFieldName);
    }

    //  ToDo:   Определиться с такими ("транзитивными") методами. Хорошо или плохо их делать?
    //  Этот метод "знает" внутренности DbFieldDefinition.
    //  Хорошо-ли это?
    //  Плюс от этого только в том, что написанного кода становиться чуть меньше в том месте,
    //  где нужно узнать результат.
    public Class<?> getClassOfDbFieldDefinition(DbFieldName dbFieldName) {
        return getDbFieldDefinition(dbFieldName).getClazz();
    }

    //  ToDo:   Определиться с такими ("транзитивными") методами. Хорошо или плохо их делать?
    //  Этот метод "знает" внутренности DbFieldDefinition.
    //  Хорошо-ли это?
    //  Плюс от этого только в том, что написанного кода становиться чуть меньше в том месте,
    //  где нужно узнать результат.
    public boolean isNullableOfDbFieldDefinition(DbFieldName dbFieldName) {
        return getDbFieldDefinition(dbFieldName).isNullable();
    }

    public boolean containsDbFieldName(DbFieldName dbFieldName) {
        return dbFieldName_DbFieldDefinition_Map.containsKey(dbFieldName);
    }

    @Override
    public String toString() {
        return "DbFields{" +
                "dbFieldName_DbFieldDefinition_Map=" + dbFieldName_DbFieldDefinition_Map +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DbFields dbFields1)) return false;
        return Objects.equals(dbFieldName_DbFieldDefinition_Map, dbFields1.dbFieldName_DbFieldDefinition_Map);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbFieldName_DbFieldDefinition_Map);
    }

    public Set<DbFieldName> getDbFieldName_Set() {
        return dbFieldName_DbFieldDefinition_Map.keySet();
    }
}
