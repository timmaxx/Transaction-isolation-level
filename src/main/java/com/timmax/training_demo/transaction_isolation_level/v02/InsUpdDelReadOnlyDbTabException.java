package com.timmax.training_demo.transaction_isolation_level.v02;

public class InsUpdDelReadOnlyDbTabException extends RuntimeException {
    public InsUpdDelReadOnlyDbTabException(String message) {
        super(message);
    }
}
