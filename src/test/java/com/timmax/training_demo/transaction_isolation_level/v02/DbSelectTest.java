package com.timmax.training_demo.transaction_isolation_level.v02;

import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.SQLCommandQueue;
import com.timmax.training_demo.transaction_isolation_level.v02.sqlcommand.dql.DQLCommandSelect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.timmax.training_demo.transaction_isolation_level.v02.DbTestData.*;

public class DbSelectTest {
    protected static final Logger logger = LoggerFactory.getLogger(DbSelectTest.class);

    @Test
    public void selectFromEmptyTable() {
        //  SELECT *
        //    FROM person
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(1L, dbTabPersonEmpty)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTable() {
        //  SELECT *
        //    FROM person
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(1L, dbTabPersonWithOneRow)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTable() {
        //  SELECT *
        //    FROM person
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(1L, dbTabPersonWithTwoRows)
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithTwoRows, dbSelect);
    }

    @Test
    public void selectFromEmptyTableWhereIdEq1() {
        //  SELECT *
        //    FROM person
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(1L, dbTabPersonEmpty, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonEmpty, dbSelect);
    }

    @Test
    public void selectFromOneRowTableWhereIdEq1() {
        //  SELECT *
        //    FROM person
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(1L, dbTabPersonWithOneRow, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }

    @Test
    public void selectFromTwoRowsTableWhereIdEq1() {
        //  SELECT *
        //    FROM person
        //   WHERE id = 1
        final SQLCommandQueue sqlCommandQueue1 = new SQLCommandQueue(
                new DQLCommandSelect(1L, dbTabPersonWithTwoRows, dbRec -> dbRec.getValue(DB_FIELD_NAME_ID).equals(1))
        );
        sqlCommandQueue1.startThread();
        sqlCommandQueue1.joinToThread();

        DbSelect dbSelect = sqlCommandQueue1.popFromDQLResultLog();

        Assertions.assertEquals(dbSelectPersonWithOneRow, dbSelect);
    }
}
