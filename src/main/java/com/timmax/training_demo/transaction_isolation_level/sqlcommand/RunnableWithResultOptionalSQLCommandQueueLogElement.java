package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import java.util.Optional;

@FunctionalInterface
public interface RunnableWithResultOptionalSQLCommandQueueLogElement {
    /**
     * Runs this operation.
     */
    Optional<SQLCommandQueueLogElement> run();
}
