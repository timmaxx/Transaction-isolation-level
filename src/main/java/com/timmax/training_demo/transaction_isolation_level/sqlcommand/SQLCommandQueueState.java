package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

public enum SQLCommandQueueState {
    IN_PREPARATION,
    STARTED,
    STOPPED,
    MALFUNCTIONED_ROLLED_BACK,// остановлен с ошибкой
    COMMITTED,
    ROLLED_BACK
}
