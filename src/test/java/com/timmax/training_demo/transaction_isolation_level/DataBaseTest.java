package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.*;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
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
                        workDbTable,
                        1,
                        oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)
                )
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void deleteOneRecordFromOneRecordTable() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue();
        sqlCommandQueue.add(
                new SQLCommandDelete(
                        workDbTable,
                        1
                )
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void insertOneRecordIntoEmptyTable_Rollback() {
//  ToDo: delete copy-paste code
//  begin like insertOneRecordIntoEmptyTable()
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
//  end like insertOneRecordIntoEmptyTable()

        sqlCommandQueue.rollback();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void updateOneRecordInOneRecordTable_Rollback() {
//  ToDo: delete copy-paste code
//  begin like updateOneRecordInOneRecordTable
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue();
        sqlCommandQueue.add(
                new SQLCommandUpdate(
                        workDbTable,
                        1,
                        oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)
                )
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
//  end like updateOneRecordInOneRecordTable
        sqlCommandQueue.rollback();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void deleteOneRecordFromOneRecordTable_Rollback() {
//  ToDo: delete copy-paste code
//  begin like deleteOneRecordFromOneRecordTable
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue();
        sqlCommandQueue.add(
                new SQLCommandDelete(
                        workDbTable,
                        1
                )
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, workDbTable);
//  end like deleteOneRecordFromOneRecordTable
        sqlCommandQueue.rollback();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void lostUpdateProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandUpdate(
                        workDbTable,
                        1,
                        oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)
                )
        );
        sqlCommandQueue1.add(
                new SQLCommandSleep(
                        null,
                        200
                ));
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(
                        null,
                        10
                ));
        sqlCommandQueue2.add(
                new SQLCommandUpdate(
                        workDbTable,
                        1,
                        oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)
                )
        );
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, workDbTable);
    }

    @Test
    public void dirtyReadProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandUpdate(
                        workDbTable,
                        1,
                        oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)
                )
        );
        sqlCommandQueue1.add(
                new SQLCommandSleep(
                        null,
                        300
                ));
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(
                        null,
                        200
                ));
        sqlCommandQueue2.add(
                new SQLCommandSelect(
                        workDbTable,
                        1
                ));
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        sqlCommandQueue1.rollback();

        DbTable middleDbTableResultInTransaction2 = new DbTable();
        middleDbTableResultInTransaction2.insert(sqlCommandQueue2.popFromDbRecordResultLog());
        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, middleDbTableResultInTransaction2);
    }
}
