package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

/*
public class Main {
    public static void main(String[] args) {
        Map<Integer, String> map = ImmutableMap.<Integer, String>builder()
            .put(3, "Three")
            .put(1, "One")
            .put(2, "Two")
            .build();

        // Порядок сохраняется
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }
    }
}
*/

public class DbFields {
    private final ImmutableMap<DbFieldName, Class<?>> dbFields;

    //  Warning:(10, 21) Raw use of parameterized class 'DbField'
    public DbFields(DbField... dbFields) {
        //  // Преобразование массива в Map с помощью Stream API
        //        Map<String, Integer> map = Arrays.stream(pairs)
        //                .collect(Collectors.toMap(
        //                        Pair::getKey,    // Функция для получения ключа
        //                        Pair::getValue   // Функция для получения значения
        //                ));
        //  Error:(42, 26) Incompatible types. Found: 'java.util.Map<com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,java.lang.Class>', required: 'java.util.Map<? extends com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,? extends java.lang.Class<?>>'
        //  Error:(42, 26) Incompatible types. Found:
        //  'java.util.Map<com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,java.lang.Class>'
        //  , required:
        //  'java.util.Map<? extends com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,? extends java.lang.Class<?>>'
        this.dbFields = ImmutableMap.<DbFieldName, Class<?>>builder()
                .putAll(Arrays
                        .stream(dbFields)
                        //  1.  Не уверен, что toMap создаст мапу упорядоченную так, как создаётся stream.
                        //      А вдруг порядок перемешается?
                        //  2.  Почему Idea подчеркивает красным collect?
                        .collect(Collectors.toMap(DbField::getDbFieldName, DbField::getType)))
                .build();
/*
        //  Это вариант с приведением типов
        //  java: incompatible types: java.util.Map<com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,java.lang.Class> cannot be converted to java.util.Map<? extends com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,? extends java.lang.Class<?>>
        //  java: incompatible types:
        //  java.util.Map<com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,java.lang.Class>
        //  cannot be converted to
        //  java.util.Map<? extends com.timmax.training_demo.transaction_isolation_level.v02.DbFieldName,? extends java.lang.Class<?>>
        this.dbFields = ImmutableMap.<DbFieldName, Class<?>>builder()
                .putAll((Map<? extends DbFieldName, ? extends Class<?>>) Arrays
                        .stream(dbFields)
                        //  1.  Не уверен, что toMap создаст мапу упорядоченную так, как создаётся stream.
                        //      А вдруг порядок перемешается?
                        //  2.  Так Idea не подчеркивает красным collect, но подчеркивает желтым и НЕ компилирует.
                        .collect(Collectors.toMap(DbField::getDbFieldName, DbField::getType)))
                .build();
*/
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

    public ImmutableMap<DbFieldName, Class<?>> getDbFields() {
        return dbFields;
    }
}
