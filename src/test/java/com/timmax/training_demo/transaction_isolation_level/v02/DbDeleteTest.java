package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbDeleteTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbDeleteTest.class);


    @Test
    public void deleteFromReadOnlyTableViaSQLCommandQueue() {
        //  DELETE
        //    FROM person   --  0 rows and table is read only
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDMLCommandDelete(dbTabPersonEmpty)
        );
        sqlCommandQueue1.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue1::joinToThread
        );

        assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  DELETE
        //    FROM person   --  0 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson.getDMLCommandDelete(dbTabPerson),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    private void deleteFromOneRowTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  1 row
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandDelete(dbTabPerson),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    private void deleteFromTwoRowsTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  2 rows
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandDelete(dbTabPerson),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    private void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandDelete(dbTabPerson,dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)),
                dbTabPerson.getDQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void deleteFromOneRowTableViaSQLCommandQueueAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromOneRowTableViaSQLCommandQueueAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableViaSQLCommandQueueAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableViaSQLCommandQueueAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueueAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueueAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabViaSQLCommandQueueAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue1, dbTabPerson);
    }
}
