package com.timmax.training_demo.transaction_isolation_level.v02;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DbRec {
    protected static final Logger logger = LoggerFactory.getLogger(DbRec.class);

    //  ToDo:   Или в этом классе ввести поле DbTab, которое будет указывать на таблицу, которой принадлежит запись.
    //          Или создать отдельный класс.
    //          Собственно из-за этого-то и была проблема с копиями записей в разных таблицах.
    //          Лучше сделать отдельные классы DbRec0, dbField0, dbFields0, DbFieldName0, DbFieldNames0, DbObjectName0,
    //          в которых ввести поля для ссылки на объект-владельца.

    //  ToDo:   Реализация задачи выше (через класс DbRec0) оказалась не эффективной. Нужно отказаться от неё.

    private final Map<DbFieldName, Object> recMap;

    public DbRec() {
        recMap = new HashMap<>();
    }

    public DbRec(Map<DbFieldName, Object> recMap) {
        this();
        this.recMap.putAll(recMap);
    }

    public DbRec(DbRec rec) {
        this(rec.recMap);
    }

    public void setAll(Map<DbFieldName, Object> newRecMap) throws SQLException {
        for (DbFieldName dbFieldName : newRecMap.keySet()) {
            if (!recMap.containsKey(dbFieldName)) {
                throw new SQLException("ERROR: column '" + dbFieldName + "' does not exist.");
            }
            Object oldValue = recMap.get(dbFieldName);
            Object newValue = newRecMap.get(dbFieldName);
            if (!oldValue.equals(newValue)) {
                recMap.put(dbFieldName, newValue);
            }
        }
    }

    public int size() {
        return recMap.size();
    }

    public Object getValue(DbFieldName fieldName) {
        return recMap.get(fieldName);
    }

    public void verify(DbFields dbFields) throws SQLException {
        StringBuilder sb = new StringBuilder("\n");
        for (Map.Entry<DbFieldName, Object> entry : recMap.entrySet()) {
            DbFieldName dbFieldName = entry.getKey();
            Object value = entry.getValue();
            if (!dbFields.containsKey(dbFieldName)) {
                sb.append("Нет поля '").append(dbFieldName).append("'\n");
            } else if (!dbFields.getDbFieldType(dbFieldName).equals(value.getClass())) {
                sb.append("Тип '")
                        .append(dbFields.getDbFieldType(dbFieldName))
                        .append("' поля '").append(dbFieldName)
                        .append("' не соответствует типу '")
                        .append(value.getClass())
                        .append("' для значения '")
                        .append(value)
                        .append("'.\n");
            }
        }
        if (!sb.toString().equals("\n")) {
            throw new SQLException(sb.toString());
        }
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
