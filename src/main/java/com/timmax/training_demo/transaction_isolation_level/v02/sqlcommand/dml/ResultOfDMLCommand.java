package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.ResultOfSQLCommand;

import java.util.Objects;

public final class ResultOfDMLCommand extends ResultOfSQLCommand {
    private final DMLCommandQueueLogElement logResult;

    public ResultOfDMLCommand(
            DMLCommandQueueLogElement logResult) {
        this.logResult = logResult;
    }

    public DMLCommandQueueLogElement getLogResult() {
        return logResult;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ResultOfDMLCommand) obj;
        return Objects.equals(this.logResult, that.logResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logResult);
    }

    @Override
    public String toString() {
        return "ResultOfDMLCommand[" +
                "logResult=" + logResult + ']';
    }
}
