package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbSQLException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@FunctionalInterface
public interface VerifyAndBuildExceptionMessageStream<T> {
    void workWithEntry(StringBuilder sb, AtomicBoolean isThereError, T entry);

    static <U> void work(
            Stream<U> dbField_Stream,
            VerifyAndBuildExceptionMessageStream<U> verifyAndBuildExceptionMessage
    ) {
        StringBuilder sb = new StringBuilder("\n");
        AtomicBoolean isThereError = new AtomicBoolean(false);

        dbField_Stream.forEach(entry -> verifyAndBuildExceptionMessage.workWithEntry(sb, isThereError, entry));

        if (isThereError.get()) {
            throw new DbSQLException(sb.toString());
        }
    }
}
