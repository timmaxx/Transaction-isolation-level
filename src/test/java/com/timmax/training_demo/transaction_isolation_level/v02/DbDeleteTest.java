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
    public void deleteFromReadOnlyTable() {
        //  DELETE
        //    FROM person   --  0 rows and table is read only
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPersonEmpty.getDMLCommandDelete()
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
    public void deleteFromEmptyTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  DELETE
        //    FROM person   --  0 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                dbTabPerson.getDMLCommandDelete(),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    private void deleteFromOneRowTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  1 row
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandDelete(),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTable(dbTabPerson, sqlCommandQueue1);
    }

    private void deleteFromTwoRowsTable(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  2 rows
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandDelete(),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTable() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTable(dbTabPerson, sqlCommandQueue1);
    }

    private void deleteFromTwoRowsTableWhereIdEq2(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        sqlCommandQueue1.add(
                dbTabPerson.getDMLCommandDelete(
                        dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
                ),
                dbTabPerson.getDQLCommandSelect()
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue1);
    }

    @Test
    public void deleteFromOneRowTableAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTable(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromOneRowTableAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTable(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableAndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTable(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableAndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTable(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonEmpty, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2AndRollback() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithTwoRows, sqlCommandQueue1, dbTabPerson);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2AndCommit() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2(dbTabPerson, sqlCommandQueue1);

        //  COMMIT;
        sqlCommandQueue1.commit();

        DbSelectUtil.selectFromDbTabAndAssertEqualsWithExpectedDbSelect(dbSelectPersonWithOneRow, sqlCommandQueue1, dbTabPerson);
    }
}
