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
                new DMLCommandDelete(1L, dbTabPersonEmpty)
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

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromEmptyTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonEmpty, false);

        //  DELETE
        //    FROM person   --  0 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        //  DELETE
        //    FROM person   --  1 row
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromOneRowTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        //  DELETE
        //    FROM person   --  1 row
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        dbTabPerson.delete();

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaMainThread() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        dbTabPerson.delete(
                dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)
        );

        DbSelect dbSelect = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueue() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson,dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void deleteFromOneRowTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithOneRow, false);

        //  DELETE
        //    FROM person   --  1 row
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
        //  Код до этой строки - копия того, что в методе
        //  void deleteFromOneRowTableViaSQLCommandQueue

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbTabPersonWithOneRow, dbSelect2);
    }

    @Test
    public void deleteFromTwoRowsTableViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
        //  Код до этой строки - копия того, что в методе
        //  void deleteFromTwoRowsTableViaSQLCommandQueue

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbTabPersonWithTwoRows, dbSelect2);
    }

    @Test
    public void deleteFromTwoRowsTableWhereIdEq2ViaSQLCommandQueueAndRollBack() {
        DbTab dbTabPerson = new DbTab(dbTabPersonWithTwoRows, false);

        //  DELETE
        //    FROM person   --  2 rows
        //   WHERE id = 2
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DMLCommandDelete(1L, dbTabPerson,dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(2)),
                new DQLCommandSelect(1L, dbTabPerson)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
        //  Код до этой строки - копия того, что в методе
        //  void deleteFromTwoRowsTableViaSQLCommandQueue

        //  ROLLBACK;
        sqlCommandQueue1.rollback();
        //  Смущает, что селект после отката сделал не через SQLCommandQueue:
        DbSelect dbSelect2 = dbTabPerson.select().getDbSelect();

        Assertions.assertEquals(dbTabPersonWithTwoRows, dbSelect2);
    }
}
