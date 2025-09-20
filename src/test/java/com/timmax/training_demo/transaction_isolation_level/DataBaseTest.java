package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommand;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandInsert;
import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandUpdate;
import com.timmax.training_demo.transaction_isolation_level.table.DbTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.timmax.training_demo.transaction_isolation_level.TestData.*;

public class DataBaseTest {
    @Test
    public void insertOneRecordIntoEmptyTable() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        SQLCommand sqlCommand = new SQLCommandInsert(
                workDbTable,
                recordForOneInsert
        );
        sqlCommand.startThread();

        sqlCommand.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void updateOneRecordInOneRecordTable() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        SQLCommand sqlCommand = new SQLCommandUpdate(
                workDbTable
        );
        sqlCommand.startThread();

        sqlCommand.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void lostUpdateProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        SQLCommand sqlCommand1 = new SQLCommandUpdate(
                workDbTable
        );
        sqlCommand1.startThread();

        SQLCommand sqlCommand2 = new SQLCommandUpdate(
                workDbTable
        );
        sqlCommand2.startThread();

        sqlCommand1.joinToThread();
        sqlCommand2.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
    }
}
