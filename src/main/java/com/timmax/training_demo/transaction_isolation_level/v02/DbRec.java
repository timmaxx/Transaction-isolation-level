package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class DbRec {
    protected static final Logger logger = LoggerFactory.getLogger(DbRec.class);

    static final String ERROR_COLUMN_DOESNT_EXIST = "ERROR: column '%s' does not exist.";
    static final String ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN = "ERROR: invalid input syntax for '%s' (column '%s'): '%s'.";

    //  Done:   Или в этом классе ввести поле DbTab, которое будет указывать на таблицу, которой принадлежит запись.
    //          Или создать отдельный класс.
    //          Собственно из-за этого-то и была проблема с копиями записей в разных таблицах.
    //          Лучше сделать отдельные классы DbRec0, dbField0, dbFields0, DbFieldName0, DbFieldNames0, DbObjectName0,
    //          в которых ввести поля для ссылки на объект-владельца.
    //  ToDo:   Реализация задачи выше (через класс DbRec0) оказалась не эффективной. Нужно отказаться от неё.

    private final Map<DbFieldName, Object> recMap;
    private final DbFields dbFields;

    public DbRec(DbFields dbFields) {
        this.dbFields = dbFields;
        recMap = new HashMap<>();
        for (DbFieldName dbFieldName : dbFields.getDbFields().keySet()) {
            recMap.put(dbFieldName, null);
        }
    }

    public DbRec(DbFields dbFields, Map<DbFieldName, Object> recMap) {
        this(dbFields);
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        recMap.entrySet().stream()
                //  recMap сортируется по ключу. Для этого
                //  class DbObjectName implements Comparable<DbObjectName>
                //  Но лучше было-бы для тех полей, которые есть в DbFields, сортировать по порядку включения,
                //  а уже те, которых нет, сортировать по имени полей.
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    DbFieldName newDbFieldName = entry.getKey();
                    Object newValue = entry.getValue();
                    if (!dbFields.containsKey(newDbFieldName)) {
                        sb.append(String.format(ERROR_COLUMN_DOESNT_EXIST, newDbFieldName)).append("\n");
                        isThereError.set(true);
                    } else if (newValue != null &&
                            !dbFields.getDbFieldType(newDbFieldName).equals(newValue.getClass())
                    ) {
                        sb.append(String.format(
                                        ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN,
                                        dbFields.getDbFieldType(newDbFieldName),
                                        newDbFieldName,
                                        newValue
                                )
                        ).append("\n");
                        isThereError.set(true);
                    }
                });

        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
        this.recMap.putAll(recMap);
    }

    //  ToDo:
    //  Warning:(81, 12) Copy constructor does not copy field 'dbFields'
    public DbRec(DbRec rec) {
        this(rec.dbFields, rec.recMap);
    }

    void setAll(Map<DbFieldName, Object> newRecMap) {
        //  ToDo:   Переделать.
        //          Здесь конструктор вызывается только для проверки. Что вероятно влечёт несколько проблем:
        //          1. Может оптимизатор вообще его не вызовет.
        //          2. Для update некоторые поля не будут в set, но при этом они не должны быть null.
        //  Warning:(90, 15) Variable 'newRec' is never used
        DbRec newRec = new DbRec(dbFields, newRecMap);
        for (DbFieldName dbFieldName : newRecMap.keySet()) {
            Object oldValue = recMap.get(dbFieldName);
            Object newValue = newRecMap.get(dbFieldName);
            if (!oldValue.equals(newValue)) {
                recMap.put(dbFieldName, newValue);
            }
        }
    }

    public Object getValue(DbFieldName dbFieldName) {
        if (!recMap.containsKey(dbFieldName)) {
            throw new DbSQLException(String.format(ERROR_COLUMN_DOESNT_EXIST, dbFieldName));
        }
        return recMap.get(dbFieldName);
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
