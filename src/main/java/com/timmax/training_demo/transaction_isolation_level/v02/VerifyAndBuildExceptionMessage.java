package com.timmax.training_demo.transaction_isolation_level.v02;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@FunctionalInterface
public interface VerifyAndBuildExceptionMessage {
    void work(StringBuilder sb, AtomicBoolean isThereError, Map.Entry<DbFieldName, Object> entry);
}
