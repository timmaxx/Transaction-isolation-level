package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.exception.DbDataAccessException;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueue;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dml.DMLCommandDelete;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLCommandSelect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTab.*;
import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbDeleteTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbDeleteTest.class);

    @Test
    public void deleteFromReadOnlyTableViaMainThread() {
        //  DELETE
        //    FROM person   --  0 rows and table is read only
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                dbTabPersonEmpty::delete
        );

        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromReadOnlyTableViaSQLCommandQueue() {
        //  DELETE
        //    FROM person   --  0 rows and table is read only
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(dbTabPersonEmpty)
        );
        sqlCommandQueue1.startThread();
        DbDataAccessException exception = Assertions.assertThrows(
                DbDataAccessException.class,
                sqlCommandQueue1::joinToThread
        );

        Assertions.assertEquals(
                String.format(ERROR_TABLE_IS_RO_YOU_CANNOT_DELETE, DB_TAB_NAME_PERSON),
                exception.getMessage(),
                EXCEPTION_MESSAGE_DOESNT_MATCH
        );
    }

    @Test
    public void deleteFromEmptyTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  DELETE
        //    FROM person   --  0 rows
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  DELETE
        //    FROM person   --  0 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(dbTabPerson),
                new DQLCommandSelect(dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        //  DELETE
        //    FROM person   --  1 row
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    private void deleteFromOneRowTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  1 row
        sqlCommandQueue1.add(
                new DMLCommandDelete(dbTabPerson),
                new DQLCommandSelect(dbTabPerson)
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

    @Test
    public void deleteFromTwoRowsTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    private void deleteFromTwoRowsTableViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  2 rows
        sqlCommandQueue1.add(
                new DMLCommandDelete(dbTabPerson),
                new DQLCommandSelect(dbTabPerson)
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

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        dbTabPerson.delete(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)
        );

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    private void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue(DbTab dbTabPerson, SQLCommandQueue sqlCommandQueue1) {
        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        sqlCommandQueue1.add(
                new DMLCommandDelete(dbTabPerson,dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).eq(2)),
                new DQLCommandSelect(dbTabPerson)
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
    public void deleteFromOneRowTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromOneRowTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithOneRow, dbSelect2);
    }

    @Test
    public void deleteFromTwoRowsTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect2);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue();

        deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue(dbTabPerson, sqlCommandQueue1);

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        DbSelectUtil.assertEquals(dbSelectPersonWithTwoRows, dbSelect2);
    }
}
