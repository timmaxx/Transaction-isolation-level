package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

import static com.timmax.training_demo.transaction_isolation_level.v02.SQLCommandQueueUtil.startAllAndJoinToAllThreads;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbDeleteTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbDeleteTest.class);

    DbTab dbTabPersonEmpty;
    DbTab dbTabPersonWithOneRow;
    DbTab dbTabPersonWithTwoRows;
    SQLCommandQueue sqlCommandQueue;
    DbSelect dbSelect;


    @BeforeEach
    public void beforeEach() {
        dbTabPersonEmpty = new DbTab(dbTabPersonRoEmpty, false);
        dbTabPersonWithOneRow = new DbTab(dbTabPersonRoWithOneRow, false);
        dbTabPersonWithTwoRows = new DbTab(dbTabPersonRoWithTwoRows, false);
        sqlCommandQueue = new SQLCommandQueue();
    }

    @Test
    public void deleteFromReadOnlyTable() {
        //  DELETE
        //    FROM person   --  0 rows and table is read only
        sqlCommandQueue.add(
                dbTabPersonRoEmpty.getDMLCommandDelete()
        );
        sqlCommandQueue.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue::joinToThread
        );

        assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromEmptyTable() {
        DbTab dbTabPerson = dbTabPersonEmpty;

        //  DELETE
        //    FROM person   --  0 rows
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandDelete(),
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    private void deleteFromOneRowTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  DELETE
        //    FROM person   --  1 row
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandDelete(),
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTable() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        deleteFromOneRowTable(dbTabPerson, sqlCommandQueue);
    }

    private void deleteFromTwoRowsTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  DELETE
        //    FROM person   --  2 rows
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandDelete(),
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTable() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        deleteFromTwoRowsTable(dbTabPerson, sqlCommandQueue);
    }

    private void deleteFromTwoRowsTableWhereIdEq2(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue) {
        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        sqlCommandQueue.add(
                dbTabPerson.getDMLCommandDelete(
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                ),
                dbTabPerson.getDQLCommandSelect()
        );
        startAllAndJoinToAllThreads(sqlCommandQueue);

        dbSelect = sqlCommandQueue.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        deleteFromTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue);
    }

    @Test
    public void deleteFromOneRowTableAndRollback() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        deleteFromOneRowTable(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void deleteFromOneRowTableAndCommit() {
        DbTab dbTabPerson = dbTabPersonWithOneRow;

        deleteFromOneRowTable(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableAndRollback() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        deleteFromTwoRowsTable(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableAndCommit() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        deleteFromTwoRowsTable(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2AndRollback() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        deleteFromTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue);

        //  ROLLBACK;
        sqlCommandQueue.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2AndCommit() {
        DbTab dbTabPerson = dbTabPersonWithTwoRows;

        deleteFromTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue);

        //  COMMIT;
        sqlCommandQueue.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue, dbTabPerson);
    }
}
