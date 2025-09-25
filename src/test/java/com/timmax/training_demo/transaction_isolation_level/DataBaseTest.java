package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.*;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
import com.timmax.training_demo.transaction_isolation_level.table.DbTable;
import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.timmax.training_demo.transaction_isolation_level.TestData.*;

public class DataBaseTest {
    protected static final Logger logger = LoggerFactory.getLogger(DataBaseTest.class);

    @Test
    public void testSelectFromEmptyTableWhereRowIdSetIsEmpty() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of())
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void testSelectFromEmptyTableWhereRowIdEquals1or2() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void testSelectFromOneRecordTableWhereRowIdSetIsEmpty() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of())
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void testSelectFromOneRecordTableWhereRowIdEquals1() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void testSelectFromTwoRecordsTableWhereRowIdSetIsEmpty() {
        final DbTable workDbTable = new DbTable(TWO_RECORDS_AFTER_TWO_INSERTS_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of())
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void testSelectFromTwoRecordsTableWhereRowIdEquals1() {
        final DbTable workDbTable = new DbTable(TWO_RECORDS_AFTER_TWO_INSERTS_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void testSelectFromTwoRecordsTableWhereRowIdEquals1or2() {
        final DbTable workDbTable = new DbTable(TWO_RECORDS_AFTER_TWO_INSERTS_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(1, 2))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(TWO_RECORDS_AFTER_TWO_INSERTS_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    //----------------------------------
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
                new SQLCommandSelect(workDbTable, Set.of(1))
        );
        sqlCommandQueue2.startThread();

        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        sqlCommandQueue1.rollback();

        ImmutableDbTable middleDbTableResultInTransaction2 = sqlCommandQueue2.popFromImmutableDbTableResultLog();
        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, middleDbTableResultInTransaction2);
    }

    @Test
    public void NonRepeatableReadProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(1)),
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, Set.of(1))
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

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue1.popFromImmutableDbTableResultLog();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        logger.debug("dbTableResultInTransaction1 = {}", dbTableResultInTransaction1);
        logger.debug("dbTableResultInTransaction2 = {}", dbTableResultInTransaction2);
        Assertions.assertNotEquals(dbTableResultInTransaction1, dbTableResultInTransaction2);
    }

    @Test
    @Disabled
    public void PhantomReadsProblemForZeroToOneRow() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);
        logger.info("workDbTable = {}", workDbTable);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(1, 2)),
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, Set.of(1, 2))
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

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue1.popFromImmutableDbTableResultLog();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        logger.info("dbTableResultInTransaction1 = {}", dbTableResultInTransaction1);
        logger.info("dbTableResultInTransaction2 = {}", dbTableResultInTransaction2);
        Assertions.assertNotEquals(dbTableResultInTransaction1.count(), dbTableResultInTransaction2.count());
    }

    @Test
    @Disabled
    public void PhantomReadsProblemForOneToTwoRows() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);
        logger.info("workDbTable = {}", workDbTable);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();
        sqlCommandQueue1.add(
                new SQLCommandSelect(workDbTable, Set.of(1, 2)),
                new SQLCommandSleep(null, 200),
                new SQLCommandSelect(workDbTable, Set.of(1, 2))
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

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue1.popFromImmutableDbTableResultLog();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        logger.info("dbTableResultInTransaction1 = {}", dbTableResultInTransaction1);
        logger.info("dbTableResultInTransaction2 = {}", dbTableResultInTransaction2);
        //  ToDo:   Сделать проверку на то, что количество строк НЕ совпадает.
        Assertions.assertNotEquals(dbTableResultInTransaction1.count(), dbTableResultInTransaction2.count());
    }
}
