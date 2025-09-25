package com.timmax.training_demo.transaction_isolation_level.sqlcommand;

import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;

import java.util.Optional;

public record LogAndDataResultOfSQLCommand(
        Optional<SQLCommandQueueLogElement> logResult,
        ImmutableDbTable dqlResult) {
}
