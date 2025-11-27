package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.ResultOfSQLCommand;

import java.util.Objects;

public final class ResultOfDMLCommand extends ResultOfSQLCommand {
    private final DMLCommandQueueLog resultLog;

    public ResultOfDMLCommand(
            DMLCommandQueueLog resultLog) {
        this.resultLog = resultLog;
    }

    public DMLCommandQueueLog getResultLog() {
        return resultLog;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ResultOfDMLCommand) obj;
        return Objects.equals(this.resultLog, that.resultLog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultLog);
    }

    @Override
    public String toString() {
        return "ResultOfDMLCommand[" +
                "resultLog=" + resultLog + ']';
    }
}
