package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.*;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
import com.timmax.training_demo.transaction_isolation_level.table.DbTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.TestData.*;

public class DataBaseTest {
    protected static final Logger logger = LoggerFactory.getLogger(DataBaseTest.class);

    @Test
    public void insertOneRecordIntoEmptyTable() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue();
        sqlCommandQueue.add(
                new SQLCommandInsert(workDbTable, recordForOneInsert)
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
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
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
                new SQLCommandDelete(workDbTable, 1)
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
                new SQLCommandInsert(workDbTable, recordForOneInsert)
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
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
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
                new SQLCommandDelete(workDbTable, 1)
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
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)),
                new SQLCommandSleep(null, 200)
        );
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(null, 10),
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
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
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)),
                new SQLCommandSleep(null, 300)
        );
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, 1)
        );
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        sqlCommandQueue1.rollback();

        DbTable middleDbTableResultInTransaction2 = new DbTable();
        middleDbTableResultInTransaction2.insert(sqlCommandQueue2.popFromDbRecordResultLog());
        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, middleDbTableResultInTransaction2);
    }

    @Test
    public void NonRepeatableReadProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, 1),
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, 1)
        );
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(null, 50),
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
        );
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();


        DbTable dbTableResultInTransaction2 = new DbTable();
        dbTableResultInTransaction2.insert(sqlCommandQueue1.popFromDbRecordResultLog());

        DbTable dbTableResultInTransaction1 = new DbTable();
        dbTableResultInTransaction1.insert(sqlCommandQueue1.popFromDbRecordResultLog());

        logger.debug("dbTableResultInTransaction1 = {}", dbTableResultInTransaction1);
        logger.debug("dbTableResultInTransaction2 = {}", dbTableResultInTransaction2);
        //  ToDo:   Сделать проверку на то, что количество строк совпадает, но строки отличаются.
        Assertions.assertNotEquals(dbTableResultInTransaction1, dbTableResultInTransaction2);
    }

    @Test
    @Disabled
    public void PhantomReadsProblemForZeroToOneRow() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, 1),
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, 1)
        );
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(null, 50),
                new SQLCommandInsert(workDbTable, recordForOneInsert)
        );
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();


        DbTable dbTableResultInTransaction2 = new DbTable();
        dbTableResultInTransaction2.insert(sqlCommandQueue1.popFromDbRecordResultLog());

        DbTable dbTableResultInTransaction1 = new DbTable();
        dbTableResultInTransaction1.insert(sqlCommandQueue1.popFromDbRecordResultLog());

        logger.info("dbTableResultInTransaction1 = {}", dbTableResultInTransaction1);
        logger.info("dbTableResultInTransaction2 = {}", dbTableResultInTransaction2);
        //  ToDo:   Сделать проверку на то, что количество строк НЕ совпадает.
        Assertions.assertNotEquals(dbTableResultInTransaction1, dbTableResultInTransaction2);
    }

    @Test
    @Disabled
    public void PhantomReadsProblemForOneToTwoRows() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, 1),
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, 1)
        );
        sqlCommandQueue1.startThread();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue();
        sqlCommandQueue2.add(
                new SQLCommandSleep(null, 50),
                new SQLCommandInsert(workDbTable, recordForOneInsert)
        );
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();


        DbTable dbTableResultInTransaction2 = new DbTable();
        dbTableResultInTransaction2.insert(sqlCommandQueue1.popFromDbRecordResultLog());

        DbTable dbTableResultInTransaction1 = new DbTable();
        dbTableResultInTransaction1.insert(sqlCommandQueue1.popFromDbRecordResultLog());

        logger.info("dbTableResultInTransaction1 = {}", dbTableResultInTransaction1);
        logger.info("dbTableResultInTransaction2 = {}", dbTableResultInTransaction2);
        //  ToDo:   Сделать проверку на то, что количество строк НЕ совпадает.
        Assertions.assertNotEquals(dbTableResultInTransaction1, dbTableResultInTransaction2);
    }
}
