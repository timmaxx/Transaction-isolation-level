package com.timmax.training_demo.transaction_isolation_level;

import com.timmax.training_demo.transaction_isolation_level.sqlcommand.*;
import com.timmax.training_demo.transaction_isolation_level.table.DbRecord;
import com.timmax.training_demo.transaction_isolation_level.table.DbTable;
import com.timmax.training_demo.transaction_isolation_level.table.ImmutableDbTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.timmax.training_demo.transaction_isolation_level.TestData.*;

public class DataBaseTest {
    @Test
    public void testSelectFromEmptyTableWhereRowIdSetIsEmpty() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
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

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue(
                new SQLCommandInsert(workDbTable, recordForOneInsert),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        ImmutableDbTable dbTableResultInTransaction = sqlCommandQueue.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, dbTableResultInTransaction);
    }

    @Test
    public void updateOneRecordInOneRecordTable() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue(
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        ImmutableDbTable dbTableResultInTransaction = sqlCommandQueue.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, dbTableResultInTransaction);
    }

    @Test
    public void deleteOneRecordFromOneRecordTable() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue(
                new SQLCommandDelete(workDbTable, 1),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        ImmutableDbTable dbTableResultInTransaction = sqlCommandQueue.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction);
    }

    @Test
    public void insertOneRecordIntoEmptyTable_Rollback() {
//  ToDo: delete copy-paste code
//  begin like insertOneRecordIntoEmptyTable()
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue(
                new SQLCommandInsert(workDbTable, recordForOneInsert),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
//  end like insertOneRecordIntoEmptyTable()

        sqlCommandQueue.rollback();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue2.startThread();
        sqlCommandQueue2.joinToThread();
        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue2.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction2);
    }

    @Test
    public void updateOneRecordInOneRecordTable_Rollback() {
//  ToDo: delete copy-paste code
//  begin like updateOneRecordInOneRecordTable
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue(
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
//  end like updateOneRecordInOneRecordTable

        sqlCommandQueue.rollback();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue2.startThread();
        sqlCommandQueue2.joinToThread();

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue2.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, dbTableResultInTransaction2);
    }

    @Test
    public void deleteOneRecordFromOneRecordTable_Rollback() {
//  ToDo: delete copy-paste code
//  begin like deleteOneRecordFromOneRecordTable
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue = new SQLCommandQueue(
                new SQLCommandDelete(workDbTable, 1),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue.startThread();
        sqlCommandQueue.joinToThread();

        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(EMPTY_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
//  end like deleteOneRecordFromOneRecordTable

        sqlCommandQueue.rollback();

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );
        sqlCommandQueue2.startThread();
        sqlCommandQueue2.joinToThread();

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue2.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE, dbTableResultInTransaction2);
    }

    @Test
    public void lostUpdateProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                //  Пауза внутри update сильно нужна для демонстрации lostUpdateProblem, но не для других проблем.
                //  И она должна быть больше, чем пауза перед стартом update в другой транзакции.
                new SQLCommandUpdate(0L, 100L, workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
        );

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandUpdate(10L, 100L, workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111)),
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2))
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue2.popFromImmutableDbTableResultLog();

        Assertions.assertEquals(ONE_RECORD_AFTER_FIRST_UPDATE_IMMUTABLE_DB_TABLE, dbTableResultInTransaction1);
    }

    @Test
    public void dirtyReadProblem() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new SQLCommandUpdate(workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
        );

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandSelect(200L, workDbTable, Set.of(1))
        );

        sqlCommandQueue1.startThread();
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

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new SQLCommandSelect(workDbTable, Set.of(1)),
                new SQLCommandSelect(200L, workDbTable, Set.of(1))
        );

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandUpdate(100L, 0L, workDbTable, 1, oldDbRecord -> new DbRecord(oldDbRecord.field1() + 111))
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue1.popFromImmutableDbTableResultLog();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertNotEquals(dbTableResultInTransaction1, dbTableResultInTransaction2);
    }

    @Test
    public void PhantomReadsProblemForZeroToOneRow() {
        final DbTable workDbTable = new DbTable(EMPTY_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2)),
                new SQLCommandSelect(200L, workDbTable, Set.of(0, 1, 2))
        );

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandInsert(100L, workDbTable, recordAfterOneUpdate)
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue1.popFromImmutableDbTableResultLog();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertNotEquals(dbTableResultInTransaction1.count(), dbTableResultInTransaction2.count());
    }

    @Test
    public void PhantomReadsProblemForOneToTwoRows() {
        final DbTable workDbTable = new DbTable(ONE_RECORD_AFTER_FIRST_INSERT_IMMUTABLE_DB_TABLE);

        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new SQLCommandSelect(workDbTable, Set.of(0, 1, 2)),
                new SQLCommandSelect(200L, workDbTable, Set.of(0, 1, 2))
        );

        final SQLCommandQueue sqlCommandQueue2 = new SQLCommandQueue(
                new SQLCommandInsert(100L, workDbTable, recordAfterOneUpdate)
        );

        sqlCommandQueue1.startThread();
        sqlCommandQueue2.startThread();
        sqlCommandQueue1.joinToThread();
        sqlCommandQueue2.joinToThread();

        ImmutableDbTable dbTableResultInTransaction2 = sqlCommandQueue1.popFromImmutableDbTableResultLog();
        ImmutableDbTable dbTableResultInTransaction1 = sqlCommandQueue1.popFromImmutableDbTableResultLog();

        Assertions.assertNotEquals(dbTableResultInTransaction1.count(), dbTableResultInTransaction2.count());
    }
}
