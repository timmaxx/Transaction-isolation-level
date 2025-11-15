package com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql;

import com.timmax.training_demo.transaction_isolation_level.v02.DbTab;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommand;

//      -   DQL команда (SELECT)
//          -   не изменяет данные, а значит НЕ порождает журнал изменения,
//          -   возвращают данные, а значит содержит результат.
//  Кроме того, сейчас имеется класс SQLCommandSleep, который вообще к SQL не относится.
public abstract class DQLCommand extends SQLCommand implements RunnableWithResultOfDQLCommand {
    public DQLCommand(Long millsBeforeRun, DbTab dbTab) {
        super(millsBeforeRun, dbTab);
    }

    @Override
    public ResultOfDQLCommand run() {
        return (ResultOfDQLCommand)super.run();
    }
}
