package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DbRec {
    protected static final Logger logger = LoggerFactory.getLogger(DbRec.class);

    static final String COLUMN_DOESNT_EXIST = "ERROR: column '%s' does not exist.";
    static final String INVALID_INPUT_SYNTAX_FOR_COLUMN = "ERROR: invalid input syntax for '%s' (column '%s'): '%s'.";

    //  ToDo:   Или в этом классе ввести поле DbTab, которое будет указывать на таблицу, которой принадлежит запись.
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
        boolean isThereError = false;

        //  ToDo:   Нужно поля проверять в определённой последовательности.
        //          Это для генерации сообщений об ошибках в определённом порядке:
        //          - либо в порядке именования столбцов,
        //          - либо в порядке создания столбцов в таблице.
/*
        TreeMap<DbFieldName, Object> recMap2 =  new TreeMap<>(recMap);
        for (Map.Entry<DbFieldName, Object> entry: recMap2.entrySet())
*/
        for (Map.Entry<DbFieldName, Object> entry: recMap.entrySet())
        {
            DbFieldName newDbFieldName = entry.getKey();
            Object newValue = entry.getValue();
            if (!dbFields.containsKey(newDbFieldName)) {
                sb.append(String.format(COLUMN_DOESNT_EXIST, newDbFieldName)).append("\n");
                isThereError = true;
            } else if (newValue != null && !dbFields.getDbFieldType(newDbFieldName).equals(newValue.getClass())) {
                sb.append(String.format(
                                INVALID_INPUT_SYNTAX_FOR_COLUMN,
                                dbFields.getDbFieldType(newDbFieldName),
                                newDbFieldName,
                                newValue
                        )
                ).append("\n");
                isThereError = true;
            }
        }
        if (isThereError) {
            throw new DbSQLException(sb.toString());
        }
        this.recMap.putAll(recMap);
    }

    public DbRec(DbRec rec) {
        this(rec.dbFields, rec.recMap);
    }

    void setAll(Map<DbFieldName, Object> newRecMap) {
        //  ToDo:   Переделать.
        //          Здесь конструктор вызывается только для проверки. Что вероятно влечёт несколько проблем:
        //          1. Может оптимизатор вообще его не вызовет.
        //          2. Для update некоторые поля не будут в set, но при этом они не должны быть null.
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
            throw new DbSQLException(String.format(COLUMN_DOESNT_EXIST, dbFieldName));
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
