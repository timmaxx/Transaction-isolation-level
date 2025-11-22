package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@FunctionalInterface
public interface VerifyAndBuildExceptionMessage {
    void workWithEntry(StringBuilder sb, AtomicBoolean isThereError, Map.Entry<DbFieldName, Object> entry);

    static void work(
            Map<DbFieldName, Object> recMap,
            VerifyAndBuildExceptionMessage verifyAndBuildExceptionMessage
    ) {
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        recMap.entrySet().stream()
                //  recMap сортируется по ключу. Для этого
                //  class DbObjectName implements Comparable<DbObjectName>
                //  Но лучше было-бы для тех полей, которые есть в DbFields, сортировать по порядку включения,
                //  а уже те, которых нет, сортировать по имени полей.
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> verifyAndBuildExceptionMessage.workWithEntry(sb, isThereError, entry))
        ;
        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
    }
}
