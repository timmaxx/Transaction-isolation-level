package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

//  ToDo:   Экземпляр класса нужно сделать глубоко неизменяемым.
public class DbRec {
    protected static final Logger logger = LoggerFactory.getLogger(DbRec.class);

    static final String ERROR_COLUMN_DOESNT_EXIST = "ERROR: column '%s' does not exist.";
    static final String ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN = "ERROR: invalid input syntax for '%s' (column '%s'): '%s'.";
    static final String ERROR_NULL_VALUE_IN_COLUMN_VIOLATES_NOT_NULL_CONSTRAINT = "ERROR: null value in column '%s' violates not-null constraint";

    private final DbFields dbFields;

    //  ToDo:   Возможно стоит выделить этот объект в отдельный класс.
    //          Он должен быть предназначен для задач манипуляций со значениями полей записи
    //          и при этом не предоставлять доступ к мапе,
    //          а также сериализацией и десериализацией управлять.
    private final Map<DbFieldName, Object> recMap;

    public DbRec(DbFields dbFields) {
        this.dbFields = dbFields;
        //  ToDo:   Этот и следующий вызовы можно было-бы спрятать в класс,
        //          который будет альтернативно реализовывать
        //          Map<DbFieldName, Object> recMap
        //          См. выше.
        recMap = new HashMap<>();
        dbFields.getDbFieldName_Set()
                .forEach(key -> recMap.put(key, null));
    }

    public DbRec(DbFields dbFields, Map<DbFieldName, Object> recMap) {
        this(dbFields);
        verifyCorrespondenceBetweenDbFieldsAndRecMap(recMap);
        this.recMap.putAll(recMap);
        verifyAreSomeFieldsNullButTheyMustBeNotNull();
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public DbRec(DbRec rec) {
        this(rec.dbFields, rec.recMap);
    }

    public DbRec(DbRec dbRec, Map<DbFieldName, Object> recMap) {
        this(dbRec);
        verifyCorrespondenceBetweenDbFieldsAndRecMap(recMap);
        this.recMap.putAll(recMap);
        verifyAreSomeFieldsNullButTheyMustBeNotNull();
    }

    //  ToDo:   В методах verify... видна похожая структура. Нужно избавиться от дублирования кода.
    private void verifyAreSomeFieldsNullButTheyMustBeNotNull() {
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        recMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    DbFieldName dbFieldName = entry.getKey();
                    boolean isNullable = dbFields.isNullableOfDbFieldDefinition(dbFieldName);
                    if (!isNullable &&
                            entry.getValue() == null
                    ) {
                        sb.append(String.format(ERROR_NULL_VALUE_IN_COLUMN_VIOLATES_NOT_NULL_CONSTRAINT, dbFieldName)).append("\n");
                        isThereError.set(true);
                    }
                })
        ;

        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
    }

    //  ToDo:   В методах verify... видна похожая структура. Нужно избавиться от дублирования кода.
    private void verifyCorrespondenceBetweenDbFieldsAndRecMap(Map<DbFieldName, Object> recMap) {
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
                    if (!dbFields.containsDbFieldName(newDbFieldName)) {
                        sb.append(String.format(ERROR_COLUMN_DOESNT_EXIST, newDbFieldName)).append("\n");
                        isThereError.set(true);
                    } else if (newValue != null &&
                            !dbFields.getClassOfDbFieldDefinition(newDbFieldName).equals(newValue.getClass())
                    ) {
                        sb.append(String.format(
                                        ERROR_INVALID_INPUT_SYNTAX_FOR_COLUMN,
                                        dbFields.getClassOfDbFieldDefinition(newDbFieldName),
                                        newDbFieldName,
                                        newValue
                                )
                        ).append("\n");
                        isThereError.set(true);
                    }
                })
        ;

        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
    }

    //  Method returns DbFieldValue, but not Object.
    //  This was done to check the correctness of the types.
    //  Also see DbFieldValue :: public boolean equals(Object o)
    public DbFieldValue getValue(DbFieldName dbFieldName) {
        if (!recMap.containsKey(dbFieldName)) {
            throw new DbSQLException(String.format(ERROR_COLUMN_DOESNT_EXIST, dbFieldName));
        }
        return new DbFieldValue(recMap.get(dbFieldName).getClass(), recMap.get(dbFieldName));
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
