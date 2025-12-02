package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.ResultOfSQLCommand;

import java.util.Objects;

public final class ResultOfDMLCommand extends ResultOfSQLCommand {
    private final DMLCommandLog dmlCommandLog;

    public ResultOfDMLCommand(
            DMLCommandLog dmlCommandLog) {
        this.dmlCommandLog = dmlCommandLog;
    }

    public DMLCommandLog getDmlCommandLog() {
        return dmlCommandLog;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ResultOfDMLCommand) obj;
        return Objects.equals(this.dmlCommandLog, that.dmlCommandLog);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dmlCommandLog);
    }

    @Override
    public String toString() {
        return "ResultOfDMLCommand[" +
                "dmlCommandLog=" + dmlCommandLog + ']';
    }
}
