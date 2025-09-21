package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.SQLCommandQueue;
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

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue();
        sqlCommandQueue.add(
                new SQLCommandInsert(
                        workDbTable,
                        recordForOneInsert
                )
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void updateOneRecordInOneRecordTable() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue();
        sqlCommandQueue.add(
                new SQLCommandUpdate(
                        workDbTable
                )
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void lostUpdateProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandUpdate(
                        workDbTable
                )
        );
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(new SQLCommandUpdate(
                workDbTable
        ));
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
    }
}
