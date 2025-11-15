package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql;

import com.timmax.training_demo.transaction_isolation_level.v02.DbSelect;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.ResultOfSQLCommand;

import java.util.Objects;

public class ResultOfDQLCommand extends ResultOfSQLCommand {
    private final DbSelect dbSelect;

    public ResultOfDQLCommand(
            DbSelect dbSelect
    ) {
        this.dbSelect = dbSelect;
    }

    public DbSelect getDbSelect() {
        return dbSelect;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ResultOfDQLCommand) obj;
        return Objects.equals(this.dbSelect, that.dbSelect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbSelect);
    }

    @Override
    public String toString() {
        return "ResultOfDQLCommand[" +
                "dbSelect=" + dbSelect + ']';
    }
}
